require('dotenv').config();

const express = require('express');
const crypto = require('crypto');
const { Pool } = require('pg');
const cron = require('node-cron');
// dynamiczny import node-fetch (działa na Node 16+; na Node 18+ jest global fetch)
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));

const app = express();
app.use((req, res, next) => {
  let data = [];
  req.on('data', chunk => data.push(chunk));
  req.on('end', () => {
    req.rawBody = Buffer.concat(data);
    next();
  });
})
app.use(express.json({
  verify: (req, res, buf) => {
    // jeśli rawBody nie złapał się wyżej, złap tu
    if (!req.rawBody) req.rawBody = Buffer.from(buf);
  }
}));

/* =========================
   ENV / KONFIG
   ========================= */
const VERIFY_TOKEN = process.env.VERIFY_TOKEN;
const APP_SECRET = process.env.APP_SECRET || '';

const WA_TOKEN     = process.env.WHATSAPP_TOKEN || null;
const WA_PHONE_ID  = process.env.WHATSAPP_PHONE_NUMBER_ID || null;

const TEMPLATE_LANG               = process.env.TEMPLATE_LANG || 'pl';
const TEMPLATE_ABSENCE_REMINDER  = process.env.TEMPLATE_ABSENCE_REMINDER || null;   // np. booking_absence_reminder_pl
const TEMPLATE_WEEKLY_SLOTS_INTRO = process.env.TEMPLATE_WEEKLY_SLOTS_INTRO || null; // np. booking_free_slots_intro_pl

const OUTBOUND_MAX_RETRIES  = Math.max(0, Number(process.env.OUTBOUND_MAX_RETRIES ?? 3));
const OUTBOUND_RETRY_BASE_MS= Math.max(100, Number(process.env.OUTBOUND_RETRY_BASE_MS ?? 800));

const ENABLE_CRON_BROADCAST = String(process.env.ENABLE_CRON_BROADCAST || 'true').toLowerCase() === 'true';
const CRON_TZ               = process.env.CRON_TZ || 'Europe/Warsaw';
const BROADCAST_BATCH_SLEEP_MS = Math.max(0, Number(process.env.BROADCAST_BATCH_SLEEP_MS || 150));

const pool = new Pool(
  process.env.DATABASE_URL
    ? {
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false }   // << wymagane na Render
      }
    : {
        user: process.env.DB_USER || 'booking_user',
        host: process.env.DB_HOST || 'localhost',
        database: process.env.DB_NAME || 'Booking',
        password: process.env.DB_PASSWORD || '',
        port: Number(process.env.DB_PORT || 5432),
      }
);

async function auditOutbound({ user_id, to_phone, message_type, body, template_name, variables, status, reason, wa_message_id }) {
  const client = await pool.connect();
  try {
    await client.query(
      `INSERT INTO outbound_messages
       (user_id, to_phone, message_type, body, template_name, variables, status, reason, wa_message_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [user_id || null, to_phone, message_type, body || null, template_name || null,
       variables ? JSON.stringify(variables) : null, status, reason || null, wa_message_id || null]
    );
  } finally { client.release(); }
}

/* =========================
   FRAZY / INTENCJE
   ========================= */
const RESERVATION_KEYWORDS = [
  'rezerwuję','rezerwuje','rezerwuj',
  'zapisz','zapisuję','zapisuje',
  'chcę miejsce','chce miejsce',
  'biorę miejsce','biore miejsce',
  'wchodzę','wchodze','wpadam'
];

const ABSENCE_KEYWORDS = [
  'nie będzie mnie','nie bedzie mnie',
  'odwołuję','odwoluje',
  'rezygnuję','rezygnuje',
  'nie dam rady','nie przyjdę','nie przyjde'
];

const PERIOD_KEYWORDS = [
  { re: /\b(do końca tygodnia)\b/, days: 'end_of_week' },
  { re: /\b(cały tydzień|tydzień)\b/, days: 7 },
  { re: /\b(dwa tygodnie)\b/, days: 14 },
  { re: /\b(miesiac|miesiąc)\b/, days: 30 }
];

const MONTHS_GEN = {
  'stycznia':1,'lutego':2,'marca':3,'kwietnia':4,'maja':5,'czerwca':6,
  'lipca':7,'sierpnia':8,'wrzesnia':9,'pazdziernika':10,'listopada':11,'grudnia':12
};

const DOW_ISO = {
  'poniedzialek':1,
  'wtorek':2,
  'sroda':3,'srode':3,
  'czwartek':4,
  'piatek':5,
  'sobota':6,'sobote':6,
  'niedziela':7,'niedziele':7
};

/* =========================
   UTILS
   ========================= */
function normalize(s = '') {
  return s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}
function formatYMD(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}
function formatHumanDate(ymd) {
  if (!ymd) return '';
  const [y, m, d] = ymd.split('-');
  return `${d}.${m}.${y}`;
}
function formatHumanTime(hhmm) {
  return hhmm ? hhmm : '(bez godz.)';
}

// Parser daty/godziny: dzis/jutro/pojutrze, dd.mm(.yyyy), HH:MM
function parseDateTime(text) {
  if (!text) return { session_date: null, session_time: null };
  const t = normalize(text);

  const now = new Date();
  let target = new Date(now);
  let timeHH = null, timeMM = null;

  if (/\bpojutrze\b/.test(t)) {
    target.setDate(target.getDate() + 2);
  } else if (/\bjutro\b/.test(t)) {
    target.setDate(target.getDate() + 1);
  } else if (/\bdzis(iaj)?\b/.test(t)) {
    // zostaje dziś
  }

  const dateMatch = t.match(/\b(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?\b/);
  if (dateMatch) {
    let [_, d, m, y] = dateMatch;
    d = parseInt(d, 10);
    m = parseInt(m, 10);
    if (!y) {
      y = now.getFullYear();
    } else {
      y = parseInt(y, 10);
      if (y < 100) y += 2000;
    }
    target = new Date(y, m - 1, d);
  }
  
  // "1 pazdziernika" / "1 wrzesnia" (rok opcjonalny)
  if (!dateMatch) {
    const mName = t.match(/\b(\d{1,2})\s+(stycznia|lutego|marca|kwietnia|maja|czerwca|lipca|sierpnia|wrzesnia|pazdziernika|listopada|grudnia)(?:\s+(\d{4}))?\b/);
    if (mName) {
      const d  = parseInt(mName[1],10);
      const mm = MONTHS_GEN[mName[2]];
      let y    = mName[3] ? parseInt(mName[3],10) : now.getFullYear();
      let cand = new Date(y, mm-1, d);
      if (!mName[3] && cand < new Date(now.getFullYear(), now.getMonth(), now.getDate())) {
        cand = new Date(y+1, mm-1, d); // jeśli już minęła → przyszły rok
      }
      target = cand;
    }
  }

  // "w środę / w srode" → najbliższe wystąpienie
  if (!dateMatch) {
    const mDow = t.match(/\bw\s+(poniedzialek|wtorek|sroda|srode|czwartek|piatek|sobota|sobote|niedziela|niedziele)\b/);
    if (mDow) {
      const wantIso  = DOW_ISO[mDow[1]];
      const todayIso = ((now.getDay() + 6) % 7) + 1; // ISO 1..7
      let add = (wantIso - todayIso + 7) % 7;
      if (add === 0) add = 7; // nie dziś, tylko następny taki dzień
      target = new Date(now);
      target.setDate(now.getDate() + add);
    }
  }
  
  for (const p of PERIOD_KEYWORDS) {
    if (p.re.test(t)) {
      const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      if (p.days === 'end_of_week') {
        const iso = ((now.getDay()+6)%7)+1; const add = 7-iso;
        return { from: today, to: new Date(today.getFullYear(), today.getMonth(), today.getDate()+add) };
      }
      return { from: today, to: new Date(today.getFullYear(), today.getMonth(), today.getDate()+p.days) };
    }
  }

  // --- "do dd[.:/-]mm[.yyyy]"  → zakres od dziś do wskazanej daty (włącznie)
  {
    const mNum = t.match(/\bdo\s+(\d{1,2})[\.:\-\/](\d{1,2})(?:[\.:\-\/](\d{2,4}))?\b/);
    if (mNum) {
      const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      const dd = parseInt(mNum[1],10), mm = parseInt(mNum[2],10);
      let yy = mNum[3] ? (Number(mNum[3]) < 100 ? 2000 + Number(mNum[3]) : Number(mNum[3])) : now.getFullYear();
      let to = new Date(yy, mm-1, dd);
      if (!mNum[3] && to < today) to = new Date(yy+1, mm-1, dd);
      return { from: today, to };
    }
  }

  // --- "do 20 pazdziernika" (gen.)  → zakres od dziś do wskazanej daty (włącznie)
  {
    const mName = t.match(/\bdo\s+(\d{1,2})\s+(stycznia|lutego|marca|kwietnia|maja|czerwca|lipca|sierpnia|wrzesnia|pazdziernika|listopada|grudnia)(?:\s+(\d{4}))?\b/);
    if (mName) {
      const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      const dd = parseInt(mName[1],10), mm = MONTHS_GEN[mName[2]];
      let yy = mName[3] ? Number(mName[3]) : now.getFullYear();
      let to = new Date(yy, mm-1, dd);
      if (!mName[3] && to < today) to = new Date(yy+1, mm-1, dd);
      return { from: today, to };
    }
  }

  const colonTimes = [...t.matchAll(/\b(\d{1,2}):(\d{2})\b/g)];
  if (colonTimes.length) {
    const last = colonTimes.at(-1);
    timeHH = parseInt(last[1], 10);
    timeMM = parseInt(last[2], 10);
  } else {
    const dotTime = t.match(/\b(?:godz(?:ina)?\s*|o\s+)(\d{1,2})\.(\d{2})\b/);
    if (dotTime) {
      timeHH = parseInt(dotTime[1], 10);
      timeMM = parseInt(dotTime[2], 10);
    }
  }

  return {
    session_date: formatYMD(target),
    session_time: timeHH != null ? `${String(timeHH).padStart(2,'0')}:${String(timeMM ?? 0).padStart(2,'0')}` : null
  };
}

function isReservationIntent(text) {
  const t = normalize(text || '');
  return RESERVATION_KEYWORDS.some(k => t.includes(normalize(k)));
}
function isAbsenceIntent(text) {
  const t = normalize(text || '');
  return ABSENCE_KEYWORDS.some(k => t.includes(normalize(k)));
}

// okno 24h (przy zwykłych odpowiedziach)
function within24h(dateObj) {
  if (!dateObj) return true;
  const diffMs = Date.now() - dateObj.getTime();
  return diffMs <= 24 * 60 * 60 * 1000;
}

// pauzy / sleep
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
async function pause(ms) { if (ms > 0) await sleep(ms); }

// „nadchodzący tydzień” = następny pon–niedz (Warszawa)
function getNextWeekRangeWarsaw() {
  const now = new Date();
  const dow = now.getDay(); // 0 nd .. 6 sb
  const daysToNextMon = ((8 - dow) % 7) || 7;
  const start = new Date(now.getFullYear(), now.getMonth(), now.getDate() + daysToNextMon);
  const end = new Date(start.getFullYear(), start.getMonth(), start.getDate() + 7); // exclusive
  const ymd = d => `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
  return { startYMD: ymd(start), endYMD: ymd(end) };
}

function formatFreeSlotsMessage(slots) {
  if (!slots.length) return 'W nadchodzącym tygodniu nie ma wolnych terminów.';
  const byDate = slots.reduce((acc, s) => {
    (acc[s.session_date] ||= []).push(s);
    return acc;
  }, {});
  const days = Object.keys(byDate).sort();

  const lines = ['Wolne terminy w nadchodzącym tygodniu:'];
  for (const d of days) {
    const niceDate = formatHumanDate(d);
    const items = byDate[d]
      .sort((a,b) => (a.session_time||'99:99').localeCompare(b.session_time||'99:99'))
      .map(s => {
        const time = s.session_time ? ` ${s.session_time}` : '';
        return `•${time} (klasa #${s.class_template_id})`;
      });
    lines.push(`${niceDate}:`);
    lines.push(...items);
  }
  return lines.join('\n');
}

/* =========================
   OUTBOUND (WhatsApp Cloud API) – retry/backoff
   ========================= */
async function sendText({ to, body, phoneNumberId = null }) {
  const phoneId = phoneNumberId || WA_PHONE_ID;
  if (!WA_TOKEN || !phoneId) {
    console.log('[OUTBOUND] Skipping send (missing WA_TOKEN or PHONE_ID)', { hasToken: !!WA_TOKEN, phoneId });
    await auditOutbound({ user_id: null, to_phone: to, message_type: 'text', body, status: 'skipped', reason: 'missing_credentials' });
    return { ok: false, reason: 'missing_config' };
  }
  const url = `https://graph.facebook.com/v20.0/${phoneId}/messages`;
  const payload = {
    messaging_product: 'whatsapp',
    to: String(to).replace(/^\+/, ''), // bez plusa
    type: 'text',
    text: { body }
  };

  let attempt = 0;
  await auditOutbound({ user_id: null, to_phone: to, message_type: 'text', body, status: 'queued' });
  while (true) {
    attempt += 1;
    try {
      const resp = await fetch(url, {
        method: 'POST',
        headers: { Authorization: `Bearer ${WA_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (resp.ok) {
        const data = await resp.json().catch(() => ({}));
        await auditOutbound({
          user_id: null, to_phone: to, message_type: 'text', body,
          status: 'sent', wa_message_id: data?.messages?.[0]?.id || null
        });
        return { ok: true, data };
      }
      const status = resp.status;
      const text = await resp.text().catch(() => '');
      const data = (() => { try { return JSON.parse(text); } catch { return { raw: text }; } })();
      const is5xx = status >= 500 && status <= 599;
      const is408 = status === 408;
      const is429 = status === 429;
      if ((is5xx || is408 || is429) && attempt <= OUTBOUND_MAX_RETRIES) {
        let delay = Math.floor(OUTBOUND_RETRY_BASE_MS * Math.pow(2, attempt - 1));
        delay += Math.floor(delay * Math.random() * 0.3);
        if (is429) {
          const ra = resp.headers.get('retry-after');
          const raMs = ra ? Number(ra) * 1000 : NaN;
          if (!Number.isNaN(raMs) && raMs > 0) delay = Math.max(delay, raMs);
        }
        console.warn(`[OUTBOUND RETRY] status=${status} attempt=${attempt}/${OUTBOUND_MAX_RETRIES} delay=${delay}ms`, data);
        await sleep(delay);
        continue;
      }
      console.error('[OUTBOUND ERROR]', { status, data, attempt });
      return { ok: false, status, data, attempt };
    } catch (e) {
      const msg = String(e?.message || e);
      const retriable = /(timeout|timed out|ECONNRESET|ENOTFOUND|EAI_AGAIN|network|fetch failed)/i.test(msg);
      if (retriable && attempt <= OUTBOUND_MAX_RETRIES) {
        let delay = Math.floor(OUTBOUND_RETRY_BASE_MS * Math.pow(2, attempt - 1));
        delay += Math.floor(delay * Math.random() * 0.3);
        console.warn(`[OUTBOUND RETRY EXC] attempt=${attempt}/${OUTBOUND_MAX_RETRIES} delay=${delay}ms reason=${msg}`);
        await sleep(delay);
        continue;
      }
      console.error('[OUTBOUND EXCEPTION]', msg, { attempt });
      await auditOutbound({ user_id: null, to_phone: to, message_type: 'text', body, status: 'error', reason: String(e?.message || e) });
      return { ok: false, reason: 'exception', error: msg, attempt };
    }
  }
}

// Szablony (business-initiated) – z retry/backoff
async function sendTemplate({ to, templateName, lang = TEMPLATE_LANG, components = [], phoneNumberId = null }) {
  const phoneId = phoneNumberId || WA_PHONE_ID;
  if (!WA_TOKEN || !phoneId) {
    console.log('[OUTBOUND-TMPL] Skipping (missing token or phoneId)');
    return { ok:false, reason:'missing_config' };
  }
  const url = `https://graph.facebook.com/v20.0/${phoneId}/messages`;
  const payload = {
    messaging_product: 'whatsapp',
    to: String(to).replace(/^\+/, ''),
    type: 'template',
    template: {
      name: templateName,
      language: { code: lang },
      components: components
    }
  };

  let attempt = 0;
  while (true) {
    attempt += 1;
    try {
      const resp = await fetch(url, {
        method:'POST',
        headers:{ Authorization:`Bearer ${WA_TOKEN}`, 'Content-Type':'application/json' },
        body: JSON.stringify(payload)
      });
      if (resp.ok) {
        const data = await resp.json().catch(()=>({}));
        return { ok:true, data };
      }
      const status = resp.status;
      const text = await resp.text().catch(()=> '');
      let data; try { data = JSON.parse(text); } catch { data = { raw:text }; }

      const is5xx = status >= 500 && status <= 599;
      const is408 = status === 408;
      const is429 = status === 429;
      if ((is5xx || is408 || is429) && attempt <= OUTBOUND_MAX_RETRIES) {
        let delay = Math.floor(OUTBOUND_RETRY_BASE_MS * Math.pow(2, attempt - 1));
        delay += Math.floor(delay * Math.random() * 0.3);
        if (is429) {
          const ra = resp.headers.get('retry-after');
          const raMs = ra ? Number(ra) * 1000 : NaN;
          if (!Number.isNaN(raMs) && raMs > 0) delay = Math.max(delay, raMs);
        }
        console.warn(`[OUTBOUND-TMPL RETRY] status=${status} attempt=${attempt}/${OUTBOUND_MAX_RETRIES} delay=${delay}ms`, data);
        await sleep(delay);
        continue;
      }
      console.error('[OUTBOUND-TMPL ERROR]', { status, data, attempt });
      return { ok:false, status, data, attempt };
    } catch (e) {
      const msg = String(e?.message || e);
      const retriable = /(timeout|timed out|ECONNRESET|ENOTFOUND|EAI_AGAIN|network|fetch failed)/i.test(msg);
      if (retriable && attempt <= OUTBOUND_MAX_RETRIES) {
        let delay = Math.floor(OUTBOUND_RETRY_BASE_MS * Math.pow(2, attempt - 1));
        delay += Math.floor(delay * Math.random() * 0.3);
        console.warn(`[OUTBOUND-TMPL RETRY EXC] attempt=${attempt}/${OUTBOUND_MAX_RETRIES} delay=${delay}ms reason=${msg}`);
        await sleep(delay);
        continue;
      }
      console.error('[OUTBOUND-TMPL EXCEPTION]', msg, { attempt });
      return { ok:false, reason:'exception', error:msg, attempt };
    }
  }
}

/* =========================
   MAPOWANIE WIADOMOŚCI / STATUSÓW
   ========================= */
function mapWhatsAppMessageToRecord(message, value) {
  const m = message || {};
  const ts = m.timestamp ? new Date(Number(m.timestamp) * 1000) : null;
  const displayNumber = value?.metadata?.display_phone_number || null;
  const phoneNumberId  = value?.metadata?.phone_number_id || null;

  const txt =
    m.text?.body ||
    m.button?.text ||
    m.interactive?.button_reply?.title ||
    m.interactive?.list_reply?.title ||
    null;

  return {
    source: 'whatsapp',
    provider_uid: m.id,
    provider_message_id: m.id,
    message_direction: 'inbound',
    message_type: m.type || 'unknown',
    from_wa_id: m.from || null,
    from_msisdn: m.from ? `+${String(m.from).replace(/^\+?/, '')}` : null,
    to_msisdn: displayNumber || phoneNumberId || null,
    text_body: txt,
    status: null,
    error_code: null,
    error_title: null,
    sent_ts: ts,
    payload_json: m,
  };
}

function mapWhatsAppStatusToRecord(statusObj, value) {
  const s = statusObj || {};
  const ts = s.timestamp ? new Date(Number(s.timestamp) * 1000) : null;
  const displayNumber = value?.metadata?.display_phone_number || null;
  const phoneNumberId  = value?.metadata?.phone_number_id || null;

  // idempotencja: (id:status:timestamp)
  const pid = `${s.id || 'unknown'}:${s.status || 'unknown'}:${s.timestamp || '0'}`;

  return {
    source: 'whatsapp',
    provider_uid: pid,
    provider_message_id: s.id || null,
    message_direction: 'inbound',
    message_type: 'status',
    from_wa_id: s.recipient_id || null,
    from_msisdn: s.recipient_id ? `+${String(s.recipient_id).replace(/^\+?/, '')}` : null,
    to_msisdn: displayNumber || phoneNumberId || null,
    text_body: null,
    status: s.status || null,
    error_code: s.errors?.[0]?.code || null,
    error_title: s.errors?.[0]?.title || null,
    sent_ts: ts,
    payload_json: s,
  };
}

/* =========================
   DB HELPERS
   ========================= */
async function insertInboxRecord(client, rec) {
  const sql = `
    INSERT INTO inbox_messages (
      source, provider_uid, provider_message_id, message_direction, message_type,
      from_wa_id, from_msisdn, to_msisdn, text_body, status, error_code, error_title, sent_ts, payload_json
    ) VALUES (
      $1,$2,$3,$4,$5,
      $6,$7,$8,$9,$10,$11,$12,$13,$14
    )
    ON CONFLICT (source, provider_uid) DO NOTHING
    RETURNING id;
  `;
  const params = [
    rec.source, rec.provider_uid, rec.provider_message_id,
    rec.message_direction, rec.message_type,
    rec.from_wa_id, rec.from_msisdn, rec.to_msisdn,
    rec.text_body, rec.status, rec.error_code, rec.error_title,
    rec.sent_ts, rec.payload_json
  ];
  await client.query(sql, params);
}

async function resolveUserIdByWa(client, wa) {
  if (!wa) return null;
  const waBare = String(wa).replace(/^\+?/, '');
  const waPlus = '+' + waBare;
  const sql = `
    SELECT id
    FROM public.users
    WHERE phone_e164 = $1
       OR phone_raw  = $2
       OR phone_raw  = $1
    ORDER BY id
    LIMIT 1
  `;
  const res = await client.query(sql, [waPlus, waBare]);
  return res.rows[0]?.id ?? null;
}

async function resolveClassTemplateIdBySlot(client, ymd /* 'YYYY-MM-DD' */) {
  if (!ymd) return { ok: false, reason: 'missing_date' };

  // 1) preferuj dokładnie 1 open slot
  const open = await client.query(`
    SELECT DISTINCT class_template_id
    FROM public.slots
    WHERE session_date = $1::date
      AND status = 'open'
  `, [ymd]);

  if (open.rows.length === 1) {
    return { ok: true, class_template_id: open.rows[0].class_template_id, via: 'open_slot_unique' };
  }
  if (open.rows.length > 1) {
    return { ok: false, reason: 'ambiguous_slots_open' };
  }

  // 2) jeśli nie ma open, sprawdź wszystkie sloty danego dnia
  const any = await client.query(`
    SELECT DISTINCT class_template_id
    FROM public.slots
    WHERE session_date = $1::date
  `, [ymd]);

  if (any.rows.length === 1) {
    return { ok: true, class_template_id: any.rows[0].class_template_id, via: 'day_unique_class' };
  }
  if (any.rows.length === 0) {
    return { ok: false, reason: 'no_slot_for_date' };
  }
  return { ok: false, reason: 'ambiguous_slots_any' };
}

async function resolveClassTemplateIdForAbsence(client, { user_id, session_date, session_time }) {
  if (!session_date) return { ok:false, reason:'missing_session_date' };

  // CASE A: mamy godzinę → najpierw klasy z takim start_time w danym dniu
  if (session_time) {
    // kandydaci wg rozkładu
    const cand = await client.query(`
      SELECT ct.id
      FROM public.class_templates ct
      JOIN public.groups g ON g.id = ct.group_id
      WHERE ct.is_active = true AND g.is_active = true
        AND ct.weekday_iso = EXTRACT(ISODOW FROM $1::date)::int
        AND ct.start_time  = $2::time
    `, [session_date, session_time]);

    if (cand.rowCount === 1) {
      return { ok:true, class_template_id: cand.rows[0].id, via:'ct_by_time' };
    }
    if (cand.rowCount > 1) {
      // spróbuj ograniczyć do enrollmentów użytkownika
      const byEnr = await client.query(`
        SELECT ct.id
        FROM public.class_templates ct
        JOIN public.groups g ON g.id = ct.group_id
        JOIN public.enrollments e ON e.class_template_id = ct.id
        WHERE ct.is_active = true AND g.is_active = true
          AND e.user_id = $1
          AND ct.weekday_iso = EXTRACT(ISODOW FROM $2::date)::int
          AND ct.start_time  = $3::time
      `, [user_id, session_date, session_time]);

      if (byEnr.rowCount === 1) {
        return { ok:true, class_template_id: byEnr.rows[0].id, via:'enrollment_by_time' };
      }
      return { ok:false, reason:'ambiguous_day_requires_time' };
    }
    // 0 kandydatów z takim czasem
    return { ok:false, reason:'no_slot_for_date' };
  }

  // CASE B: brak godziny → najpierw enrollmenty usera w tym dniu tygodnia
  const enr = await client.query(`
    SELECT ct.id
    FROM public.enrollments e
    JOIN public.class_templates ct ON ct.id = e.class_template_id
    JOIN public.groups g ON g.id = ct.group_id
    WHERE e.user_id = $1
      AND ct.is_active = true AND g.is_active = true
      AND ct.weekday_iso = EXTRACT(ISODOW FROM $2::date)::int
  `, [user_id, session_date]);

  if (enr.rowCount === 1) {
    return { ok:true, class_template_id: enr.rows[0].id, via:'enrollment_by_day' };
  }
  if (enr.rowCount > 1) {
    return { ok:false, reason:'ambiguous_day_requires_time' };
  }

  // brak enrollmentów — sprawdź, czy jest jedna klasa tego dnia
  const onlyCt = await client.query(`
    SELECT ct.id
    FROM public.class_templates ct
    JOIN public.groups g ON g.id = ct.group_id
    WHERE ct.is_active = true AND g.is_active = true
      AND ct.weekday_iso = EXTRACT(ISODOW FROM $1::date)::int
  `, [session_date]);

  if (onlyCt.rowCount === 1) {
    return { ok:true, class_template_id: onlyCt.rows[0].id, via:'ct_by_day' };
  }
  if (onlyCt.rowCount === 0) {
    return { ok:false, reason:'no_slot_for_date' };
  }
  return { ok:false, reason:'ambiguous_day_requires_time' };
}

// sprawdza: user.level_id ≥ group.level_id oraz max_home_price ≥ cena zajęć
async function checkReservationEligibility(client, userId, classTemplateId) {
  const q = `
    SELECT 
      u.level_id            AS user_level,
      v.max_home_price      AS max_home_price,
      g.level_id            AS required_level,
      pt.per_session_price  AS class_price
    FROM public.users u
    LEFT JOIN public.v_user_home_price v ON v.user_id = u.id
    JOIN public.class_templates ct ON ct.id = $2
    JOIN public.groups g          ON g.id = ct.group_id
    JOIN public.price_tiers pt    ON pt.id = g.price_tier_id
    WHERE u.id = $1
    LIMIT 1
  `;
  const { rows } = await client.query(q, [userId, classTemplateId]);
  if (!rows.length) return { ok:false, reason:'missing_user_or_class' };
  const r = rows[0];
  if (r.user_level < r.required_level) return { ok:false, reason:'level_too_low', req:r.required_level, have:r.user_level };
  if (r.max_home_price == null)       return { ok:false, reason:'no_home_price' };
  if (Number(r.max_home_price) < Number(r.class_price)) 
    return { ok:false, reason:'price_too_low', need:r.class_price, have:r.max_home_price };
  return { ok:true };
}

async function ensureOpenSlotForAbsence(client, absenceId, classTemplateId, ymd) {
  if (!absenceId || !classTemplateId || !ymd) return { created: false, reason: 'missing_params' };
  const sql = `
    INSERT INTO public.slots (class_template_id, session_date, source_absence_id, status)
    VALUES ($1, $2::date, $3, 'open')
    ON CONFLICT (source_absence_id) DO NOTHING
    RETURNING id;
  `;
  const { rows } = await client.query(sql, [classTemplateId, ymd, absenceId]);
  return { created: !!rows.length, slot_id: rows[0]?.id || null };
}

async function insertAbsenceRangeIfPossible(client, { userId, fromDate, toDate, session_time }) {
  let inserted = 0, skipped = 0;
  const start = new Date(fromDate);
  const end   = new Date(toDate);
  for (let d = new Date(start); d <= end; d.setDate(d.getDate()+1)) {
    const ymd = formatYMD(d);
    const r = await resolveClassTemplateIdForAbsence(client, {
      user_id: userId,
      session_date: ymd,
      session_time: session_time || null
    });
    if (!r.ok) { skipped++; continue; }

    const classTemplateId = r.class_template_id;
    const ins = await client.query(
      `INSERT INTO public.absences (user_id, class_template_id, session_date, reason)
       VALUES ($1,$2,$3::date,$4)
       ON CONFLICT (user_id, class_template_id, session_date)
       DO UPDATE SET reason = EXCLUDED.reason, updated_at = NOW()
       RETURNING id;`,
      [userId, classTemplateId, ymd, 'range']
    );
    const absenceId = ins.rows[0]?.id || null;
    await ensureOpenSlotForAbsence(client, absenceId, classTemplateId, ymd);
    inserted++;
  }
  return { inserted, skipped, range: true };
}

async function insertAbsenceIfPossible(client, candidate, fromWaId) {
  const userId = await resolveUserIdByWa(client, fromWaId);
  if (!userId) return { inserted: false, reason: 'missing_user_mapping', fromWaId };

  if (candidate?.date_from && candidate?.date_to) {
    return await insertAbsenceRangeIfPossible(client, {
      userId,
      fromDate: candidate.date_from,
      toDate:   candidate.date_to,
      session_time: candidate.session_time || null
    });
  }

  if (!userId) return { inserted: false, reason: 'missing_user_mapping', fromWaId };
  if (!candidate?.session_date) return { inserted: false, reason: 'missing_session_date' };

  const r = await resolveClassTemplateIdForAbsence(client, {
    user_id: userId,
    session_date: candidate.session_date,
    session_time: candidate.session_time
  });
  if (!r.ok) return { inserted: false, reason: r.reason || 'missing_class_template_id' };
  const classTemplateId = r.class_template_id;

  const sql = `
    INSERT INTO public.absences (user_id, class_template_id, session_date, reason)
    VALUES ($1, $2, $3::date, $4)
    ON CONFLICT (user_id, class_template_id, session_date)
    DO UPDATE SET reason = EXCLUDED.reason, updated_at = NOW()
    RETURNING id;
  `;
  const params = [userId, classTemplateId, candidate.session_date, candidate.reason];
  const { rows } = await client.query(sql, params);
  const absenceId = rows[0]?.id;

  const slot = await ensureOpenSlotForAbsence(client, absenceId, classTemplateId, candidate.session_date);

  return {
    inserted: !!rows.length,
    absence_id: absenceId || null,
    user_id: userId,
    class_template_id: classTemplateId,
    via: r.via || 'slot_match',
    slot_created: slot.created,
    slot_id: slot.slot_id || null,
  };
}

async function reserveOpenSlot(client, { user_id, class_template_id, session_date, session_time }) {
  await client.query('BEGIN');
  try {
    // 1) Odczytaj otwarte sloty na daną datę w ramach danego class_template,
    //    razem z godziną z class_templates.start_time (alias jako session_time).
    //    UWAGA: nie odwołujemy się nigdzie do slots.session_time – takiej kolumny nie ma.
    let res;

    if (session_time) {
      // Mamy godzinę → filtruj po start_time
      // Załóżmy, że upstream podaje "HH:MM" (ew. możesz tu jeszcze ustandaryzować input).
      res = await client.query(
        `
          SELECT s.id
          FROM public.slots AS s
          JOIN public.class_templates AS ct
            ON ct.id = s.class_template_id
          WHERE s.class_template_id = $1
            AND s.session_date     = $2
            AND s.status           = 'open'
            AND ct.start_time      = $3::time
          ORDER BY ct.start_time NULLS LAST, s.id
          FOR UPDATE OF s SKIP LOCKED
          LIMIT 1
        `,
        [class_template_id, session_date, session_time]
      );

      if (res.rowCount === 0) {
        await client.query('ROLLBACK');
        return { ok: false, reason: 'no_slot_for_time' }; // brak slotu o podanej godzinie
      }
    } else {
      // Brak godziny → sprawdź, ile jest otwartych slotów w tym dniu
      const allOpen = await client.query(
        `
          SELECT s.id, ct.start_time AS session_time
          FROM public.slots AS s
          JOIN public.class_templates AS ct
            ON ct.id = s.class_template_id
          WHERE s.class_template_id = $1
            AND s.session_date     = $2
            AND s.status           = 'open'
          ORDER BY ct.start_time NULLS LAST, s.id
          FOR UPDATE OF s SKIP LOCKED
        `,
        [class_template_id, session_date]
      );

      if (allOpen.rowCount === 0) {
        await client.query('ROLLBACK');
        return { ok: false, reason: 'no_open_slot_for_date' };
      }
      if (allOpen.rowCount > 1) {
        await client.query('ROLLBACK');
        return { ok: false, reason: 'ambiguous_day_requires_time' }; // jest kilka slotów – trzeba podać godzinę
      }

      // Dokładnie jeden otwarty slot → rezerwujemy ten
      res = { rowCount: 1, rows: [ { id: allOpen.rows[0].id } ] };
    }

    const slotId = res.rows[0].id;

    // 2) Zapobiegnij duplikatom rezerwacji użytkownika w tym dniu/CT
    const dup = await client.query(
      `
        SELECT 1
        FROM public.slots
        WHERE class_template_id = $1
          AND session_date      = $2
          AND taken_by_user_id  = $3
          AND status            = 'taken'
        LIMIT 1
      `,
      [class_template_id, session_date, user_id]
    );
    if (dup.rowCount > 0) {
      await client.query('ROLLBACK');
      return { ok: true, slot_id: slotId, via: 'already_taken_by_user' };
    }

    // 3) Zajmij slot (wyścig zabezpieczony FOR UPDATE SKIP LOCKED + status check)
    const upd = await client.query(
      `
        UPDATE public.slots
        SET status = 'taken',
            taken_by_user_id = $1,
            taken_at = NOW()
        WHERE id = $2
          AND status = 'open'
        RETURNING id
      `,
      [user_id, slotId]
    );

    if (upd.rowCount === 0) {
      await client.query('ROLLBACK');
      return { ok: false, reason: 'race_lost' };
    }

    await client.query('COMMIT');
    return { ok: true, slot_id: slotId, via: 'reserved' };
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  }
}

// wszyscy z numerem E.164 (rozważ dodać kolumnę users.is_opted_in = true i filtrować)
// 📋 Lista aktywnych użytkowników z numerem i imieniem
async function getAllRecipients(client) {
  const sql = `
    SELECT 
      id AS user_id,
      phone_e164 AS e164,
      first_name
    FROM public.users
    WHERE 
      phone_e164 IS NOT NULL
      AND is_active = true
  `;
  const { rows } = await client.query(sql);

  return rows.map(r => ({
    to: String(r.e164).replace(/^\+/, ''),
    user_id: r.user_id,
    first_name: r.first_name
  }));
}

// stworzenie wolnych slotów wynikających z wolnych miejsc na grupach (brak zapisanych na stale uzytkowników)
async function ensureWeeklyOpenSlots() {
  const client = await pool.connect();
  try {
    const sql = `
      WITH dates AS (
        SELECT (CURRENT_DATE + i) AS d FROM generate_series(1,7) AS i  -- najbliższy tydzień (pon–nd)
      ),
      ct AS (
        SELECT ct.id AS class_template_id, ct.weekday_iso, g.max_capacity AS capacity
        FROM class_templates ct
        JOIN groups g ON g.id = ct.group_id
        WHERE ct.is_active = true AND g.is_active = true
      ),
      ct_dates AS (
        SELECT ct.class_template_id, d::date AS session_date, ct.capacity
        FROM ct JOIN dates ON ct.weekday_iso = EXTRACT(ISODOW FROM d)::int
      ),
      enr AS (
        SELECT class_template_id, COUNT(DISTINCT user_id) AS enrolled
        FROM enrollments GROUP BY class_template_id
      ),
      existing AS (
        SELECT class_template_id, session_date,
            COUNT(*) FILTER (WHERE state='open' AND source_absence_id IS NULL) AS open_generated
        FROM slots
        WHERE session_date BETWEEN CURRENT_DATE + 1 AND CURRENT_DATE + 7
        GROUP BY class_template_id, session_date
      ),
      need AS (
        SELECT
          cd.class_template_id,
          cd.session_date,
          GREATEST(cd.capacity - COALESCE(e.enrolled,0) - COALESCE(x.open_generated,0), 0) AS to_create
        FROM ct_dates cd
        LEFT JOIN enr e ON e.class_template_id = cd.class_template_id
        LEFT JOIN existing x ON x.class_template_id = cd.class_template_id AND x.session_date = cd.session_date
      )
      INSERT INTO slots (class_template_id, session_date, state, user_id, source_absence_id)
      SELECT n.class_template_id, n.session_date, 'open', NULL, NULL
      FROM need n
      JOIN LATERAL generate_series(1, n.to_create) gs(i) ON true;
    `;
    await client.query('BEGIN');
    const res = await client.query(sql);
    await client.query('COMMIT');
    console.log('[WEEKLY SLOTS] inserted baseline opens:', res.rowCount);
    } catch (e) {
    await client.query('ROLLBACK');
    console.error('[WEEKLY SLOTS][ERROR]', e);
   } finally {
    client.release();
  }
}

// wolne sloty w nadchodzącym tygodniu
async function getOpenSlotsNextWeek(client) {
  const { startYMD, endYMD } = getNextWeekRangeWarsaw();
  const colsRes = await client.query(`
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='public' AND table_name='slots'
  `);
  const hasSessionTime = colsRes.rows.some(r => r.column_name === 'session_time');

  const sql = `
    SELECT id, class_template_id, session_date${hasSessionTime ? ', session_time' : ', NULL::text AS session_time'}
    FROM public.slots
    WHERE status = 'open'
      AND session_date >= $1::date
      AND session_date <  $2::date
    ORDER BY session_date ASC, ${hasSessionTime ? 'session_time NULLS LAST,' : ''} id ASC
    LIMIT 500
  `;
  const { rows } = await client.query(sql, [startYMD, endYMD]);
  return rows;
}

// Tworzy sloty na horyzoncie [jutro .. dziś + daysAhead] (domyślnie 14 dni)
// Idempotentnie: nie tworzy duplikatów istniejących (class_template_id + session_date)
// Tworzy open-sloty tylko gdy są wolne miejsca (capacity > enrollments)
async function createSlotsForRollingHorizon(client, daysAhead = 14) {
  const sql = `
    WITH horizon AS (
      SELECT (CURRENT_DATE + gs.i)::date AS d
      FROM generate_series(1, $1::int) AS gs(i)
    ),
    cap AS (
      SELECT ct.id AS class_template_id,
             ct.weekday_iso,
             g.max_capacity AS capacity,
             COALESCE(cnt.enrolled, 0) AS enrolled
      FROM public.class_templates ct
      JOIN public.groups g ON g.id = ct.group_id
      LEFT JOIN (
        SELECT e.class_template_id, COUNT(*)::int AS enrolled
        FROM public.enrollments e
        GROUP BY e.class_template_id
      ) AS cnt ON cnt.class_template_id = ct.id
      WHERE ct.is_active = TRUE AND g.is_active = TRUE
    ),
    to_insert AS (
      SELECT c.class_template_id, h.d AS session_date
      FROM horizon h
      JOIN cap c ON c.weekday_iso = EXTRACT(ISODOW FROM h.d)::int
      WHERE (c.capacity - c.enrolled) > 0                              -- jest wolne miejsce
        AND NOT EXISTS (                                               -- brak jakiegokolwiek slota na ten dzień
          SELECT 1 FROM public.slots s
           WHERE s.class_template_id = c.class_template_id
             AND s.session_date      = h.d
        )
    )
    INSERT INTO public.slots (class_template_id, session_date, status)
    SELECT class_template_id, session_date, 'open'
    FROM to_insert
    RETURNING id;
  `;
  const { rows } = await client.query(sql, [daysAhead]);
  return { inserted: rows.length, daysAhead };
}

async function cleanupOpenSlots(client, retainDays = 60) {
  const sql = `
    DELETE FROM public.slots
    WHERE status = 'open'
      AND session_date < CURRENT_DATE - ($1::int * INTERVAL '1 day')
    RETURNING id;
  `;
  const res = await client.query(sql, [retainDays]);
  return { deleted: res.rowCount };
}

async function sendUnrecognizedAck({ to, phoneNumberId = null }) {
  const body = '📩 Otrzymaliśmy Twoją wiadomość. Nie potrafię jej automatycznie zinterpretować — przekażę ją do administratora.';
  return sendText({ to, body, phoneNumberId });
}

/* =========================
   WHATSAPP WEBHOOK
   ========================= */
app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    console.log('Webhook verified');
    return res.status(200).send(challenge);
  }
  console.warn('Webhook verify failed:', { mode, tokenOk: token === VERIFY_TOKEN });
  return res.sendStatus(403);
});

// (opcjonalnie) podpis X-Hub-Signature-256
function verifyMetaSignature(req) {
  if (!APP_SECRET) return true;
  const sig = req.get('X-Hub-Signature-256');
  if (!sig) return false;
  const hmac = crypto.createHmac('sha256', APP_SECRET);
  const digest = 'sha256=' + hmac.update(JSON.stringify(req.body)).digest('hex');
  return crypto.timingSafeEqual(Buffer.from(digest), Buffer.from(sig));
}

app.post('/webhook', async (req, res) => {
  // (opcjonalnie) wymuś podpis, jeśli APP_SECRET jest ustawiony
  if (APP_SECRET) {
    const sigHeader = req.get('x-hub-signature-256') || '';
    const [prefix, sigHex] = sigHeader.split('=');

    // podstawowe sanity checks
    if (prefix !== 'sha256' || !sigHex) {
      return res.status(403).send('Missing signature');
    }
    // policz oczekiwany podpis
    const expectedHex = crypto
      .createHmac('sha256', APP_SECRET)
      .update(req.rawBody || Buffer.from([]))
      .digest('hex');

    // porównujemy BAJTY (hex -> Buffer)
    const a = Buffer.from(sigHex, 'hex');
    const b = Buffer.from(expectedHex, 'hex');

    // zanim użyjemy timingSafeEqual, długości muszą się zgadzać
    if (a.length !== b.length) {
      return res.status(403).send('Bad signature');
    }
    const ok = crypto.timingSafeEqual(a, b);
    if (!ok) {
      return res.status(403).send('Bad signature');
    }
  }
  console.log('[WEBHOOK HIT] headers.x-hub-signature-256=', req.get('x-hub-signature-256'));
  console.log('[WEBHOOK HIT] rawBody.len=', req.rawBody?.length, ' bodyIsArrayEntry=', Array.isArray(req.body?.entry));

  let body;
  try {
    if (req.body && Object.keys(req.body).length) {
      body = req.body;
    } else if (req.rawBody?.length) {
      body = JSON.parse(req.rawBody.toString('utf8'));
    } else {
      body = null;
    }
  } catch (e) {
    console.error('[WEBHOOK RAW JSON PARSE ERROR]', e?.message);
    return res.status(400).send('Bad JSON');
  }

  console.log('[WEBHOOK BODY PREVIEW]', typeof body, 'keys=', Object.keys(body || {}));

  if (!body || !Array.isArray(body.entry)) {
    console.log('[WEBHOOK] brak entry[] → 200');
    return res.sendStatus(200);
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (const entry of body.entry) {
      const changes = entry?.changes || [];
      for (const ch of changes) {
        const v = ch?.value || {};
        const phoneNumberIdFromHook = v?.metadata?.phone_number_id || null;
        
        console.log('[WEBHOOK CHANGE]', 'hasMessagesArray=', Array.isArray(v.messages), 'hasStatusesArray=', Array.isArray(v.statuses));

        // messages
        if (Array.isArray(v.messages)) {
          for (const m of v.messages) {
            const rec = mapWhatsAppMessageToRecord(m, v);
            await insertInboxRecord(client, rec);

            const text = rec.text_body || '';
            if (!text) continue;

            const canReplyNow = within24h(rec.sent_ts);

            // REZERWACJA
            if (isReservationIntent(text)) {
              const { session_date, session_time } = parseDateTime(text);
              if (!session_date) {
                if (canReplyNow) await sendText({
                  to: rec.from_wa_id,
                  body: '❔ Podaj proszę datę (np. 21.09) i ewentualnie godzinę (np. 19:00), żebym mógł zapisać Cię na zajęcia.',
                  phoneNumberId: phoneNumberIdFromHook
                });
                continue;
              }
              const userId = await resolveUserIdByWa(client, rec.from_wa_id);
              if (!userId) {
                if (canReplyNow) await sendText({
                  to: rec.from_wa_id,
                  body: '❗ Nie rozpoznaję Twojego numeru w systemie. Daj znać recepcji, aby Cię dodać.',
                  phoneNumberId: phoneNumberIdFromHook
                });
                continue;
              }
              let cls;
              if (session_time) {
                const ctByTime = await client.query(`
                  SELECT ct.id
                  FROM public.class_templates ct
                  JOIN public.groups g ON g.id = ct.group_id
                  WHERE ct.is_active = true AND g.is_active = true
                    AND ct.weekday_iso = EXTRACT(ISODOW FROM $1::date)::int
                    AND ct.start_time  = $2::time
                `, [session_date, session_time]);

                if (ctByTime.rowCount === 1) {
                  cls = { ok: true, class_template_id: ctByTime.rows[0].id, via: 'ct_by_time' };
                } else if (ctByTime.rowCount > 1) {
                  cls = { ok: false, reason: 'ambiguous_by_time' };
                } else {
                  // brak klasy o tej godzinie – fallback do dotychczasowej logiki po dniu
                  cls = await resolveClassTemplateIdBySlot(client, session_date);
                }
              } else {
                cls = await resolveClassTemplateIdBySlot(client, session_date);
              }
              // jeśli dzień niejednoznaczny / brak dopasowania – przerwij z komunikatem
              if (!cls.ok) {
                if (canReplyNow) {
                  const why =
                    (cls.reason === 'ambiguous_slots_open' || cls.reason === 'ambiguous_slots_any')
                      ? 'tego dnia jest kilka różnych zajęć'
                      : (cls.reason === 'ambiguous_by_time')
                        ? 'o tej godzinie są różne zajęcia – podaj proszę nazwę grupy'
                        : (cls.reason === 'no_slot_for_date')
                          ? 'tego dnia nie ma żadnych zajęć'
                          : 'brakuje danych';
                  await sendText({
                    to: rec.from_wa_id,
                    body: `❔ Nie mogę rozpoznać zajęć na ${formatHumanDate(session_date)} – ${why}. Podaj proszę dokładną godzinę lub nazwę zajęć.`,
                    phoneNumberId: phoneNumberIdFromHook
                  });
                }
                continue;
              }
              // WALIDACJA poziomu i ceny
              const elig = await checkReservationEligibility(client, userId, cls.class_template_id);
              if (!elig.ok) {
                if (canReplyNow) {
                  const msg =
                    elig.reason === 'level_too_low' ? '❗ Twój poziom jest niższy niż wymagany dla tych zajęć.' :
                    elig.reason === 'price_too_low' ? '❗ Te zajęcia są droższe niż Twój limit cenowy.' :
                      '❗ Nie mogę potwierdzić uprawnień do rezerwacji.';
                  await sendText({ to: rec.from_wa_id, body: msg, phoneNumberId: phoneNumberIdFromHook });
                }
                continue;
              }
              const r = await reserveOpenSlot(client, {
                user_id: userId,
                class_template_id: cls.class_template_id,
                session_date,
                session_time
              });

              if (canReplyNow) {
                if (r.ok && r.via === 'reserved') {
                  await sendText({
                    to: rec.from_wa_id,
                    body: `✔️ Zarezerwowane: ${formatHumanDate(session_date)} ${formatHumanTime(session_time)}.`,
                    phoneNumberId: phoneNumberIdFromHook
                  });
                } else if (r.ok && r.via === 'already_taken_by_user') {
                  await sendText({
                    to: rec.from_wa_id,
                    body: `ℹ️ Już masz rezerwację na ${formatHumanDate(session_date)} ${formatHumanTime(session_time)}.`,
                    phoneNumberId: phoneNumberIdFromHook
                  });
                } else if (!r.ok && r.reason === 'no_open_slot_match') {
                  await sendText({
                    to: rec.from_wa_id,
                    body: `❗ Brak wolnych miejsc na ${formatHumanDate(session_date)} ${formatHumanTime(session_time)}.`,
                    phoneNumberId: phoneNumberIdFromHook
                  });
                } else if (!r.ok && r.reason === 'race_lost') {
                  await sendText({
                    to: rec.from_wa_id,
                    body: `⚠️ Ktoś właśnie zajął ostatnie miejsce na ${formatHumanDate(session_date)} ${formatHumanTime(session_time)}. Spróbować inny termin?`,
                    phoneNumberId: phoneNumberIdFromHook
                  });
                } else {
                  await sendText({
                    to: rec.from_wa_id,
                    body: `❗ Nie udało się zapisać. Napisz proszę datę i godzinę – spróbuję ponownie.`,
                    phoneNumberId: phoneNumberIdFromHook
                  });
                }
              }
              continue;
            }

            // ABSENCJA
            if (isAbsenceIntent(text)) {
              const dt = parseDateTime(text);
              const candidate = (dt.from && dt.to)
                ? {
                    date_from: formatYMD(dt.from),
                    date_to:   formatYMD(dt.to),
                    session_time: dt.session_time || null,
                    reason: text.trim()
                  }
                : {
                    session_date: dt.session_date,
                    session_time: dt.session_time,
                    reason: text.trim()
                  };

              if (!candidate.session_date && !(candidate.date_from && candidate.date_to)) {
                if (canReplyNow) await sendText({
                  to: rec.from_wa_id,
                  body: '❔ Podaj proszę datę (np. 21.09) i ewentualnie godzinę (np. 19:00), żebym mógł odwołać Twoje miejsce.',
                  phoneNumberId: phoneNumberIdFromHook
                });
                continue;
              }
              const result = await insertAbsenceIfPossible(client, candidate, rec.from_wa_id);

              if (canReplyNow) {
                if (result.inserted) {
                  const info = (candidate.date_from && candidate.date_to)
                    ? `od ${formatHumanDate(candidate.date_from)} do ${formatHumanDate(candidate.date_to)}`
                    : `${formatHumanDate(candidate.session_date)} ${formatHumanTime(candidate.session_time)}`;
                  await sendText({
                    to: rec.from_wa_id,
                    body: `✔️ Zgłoszona nieobecność: ${info}. Miejsce oddane do puli.`,
                    phoneNumberId: phoneNumberIdFromHook
                  });
                } else if (result.reason === 'missing_user_mapping') {
                  await sendText({
                    to: rec.from_wa_id,
                    body: '❗ Nie rozpoznaję Twojego numeru w systemie. Daj znać recepcji, aby Cię dodać.',
                    phoneNumberId: phoneNumberIdFromHook
                  });
                } else if (['ambiguous_slots_open','ambiguous_slots_any','no_slot_for_date','ambiguous_day_requires_time'].includes(result.reason)) {
                  const why = (result.reason === 'no_slot_for_date')
                    ? 'tego dnia nie ma żadnych zajęć'
                    : 'tego dnia jest kilka różnych zajęć';
                  const dateInfo = (candidate.date_from && candidate.date_to)
                    ? `zakres ${formatHumanDate(candidate.date_from)}–${formatHumanDate(candidate.date_to)}`
                    : formatHumanDate(candidate.session_date);
                  await sendText({
                    to: rec.from_wa_id,
                    body: `❔ Nie mogę jednoznacznie przypisać zajęć na ${dateInfo} – ${why}. Podaj proszę godzinę lub nazwę zajęć.`,
                    phoneNumberId: phoneNumberIdFromHook
                  });
                } else if (result.reason === 'missing_session_date') {
                  await sendText({
                    to: rec.from_wa_id,
                    body: '❔ Podaj proszę datę (np. 21.09), żebym mógł odwołać Twoje miejsce.',
                    phoneNumberId: phoneNumberIdFromHook
                  });
                } else {
                  await sendText({
                    to: rec.from_wa_id,
                    body: '❗ Nie udało się odwołać miejsca. Napisz proszę datę i godzinę – spróbuję ponownie.',
                    phoneNumberId: phoneNumberIdFromHook
                  });
                }
              }
            }

            // --- FALLBACK: potwierdzenie dla nieznanej treści ---------------------------
            if (canReplyNow && text && !isReservationIntent(text) && !isAbsenceIntent(text)) {
              await sendUnrecognizedAck({
                to: rec.from_wa_id,
                phoneNumberId: phoneNumberIdFromHook
              });
            }
          } // end for each message
        }
      // jeśli numer nie jest znany w systemie → uprzejmy komunikat i koniec
      const knownUserId = await resolveUserIdByWa(client, rec.from_wa_id);
      if (!knownUserId) {
        if (canReplyNow) {
          await sendText({
          to: rec.from_wa_id,
          body: '📩 Otrzymaliśmy Twoją wiadomość, ale ten numer nie jest przypisany do żadnego uczestnika. Skontaktuj się z administratorem, aby dodać numer do systemu.',
        phoneNumberId: phoneNumberIdFromHook
      });
  }
  continue; // pomiń dalszą logikę (rezerwacje/nieobecności/fallback)
}
        // statuses
        if (Array.isArray(v.statuses)) {
          for (const s of v.statuses) {
            const rec = mapWhatsAppStatusToRecord(s, v);
            await insertInboxRecord(client, rec);
          }
        }
      }
    }

    await client.query('COMMIT');
    return res.sendStatus(200);
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('Webhook insert error:', e);
    return res.sendStatus(500);
  } finally {
    client.release();
  }
});

/* =========================
   CRON – sobota/niedziela z SZABLONAMI
   ========================= */
// 🔔 Sobota 16:00 – wysyłka szablonu "absence_reminder"
async function broadcastAskAbsencesTemplate(client, phoneNumberIdOverride = null) {
  const recipients = await getAllRecipients(client);
  if (!recipients.length) {
    console.log('[BROADCAST] ask-absences (tmpl): brak odbiorców');
    return;
  }

  // Nazwa szablonu z ENV
  const templateName = process.env.TEMPLATE_ABSENCE_REMINDER || 'absence_reminder';
  console.log(`[BROADCAST] ask-absences (tmpl) → ${recipients.length} osób, template=${templateName}`);

  for (const r of recipients) {
    // spróbuj wydobyć imię z obiektu użytkownika
    const firstName =
      r.first_name ||
      r.firstName ||
      (r.name ? String(r.name).split(/\s+/)[0] : null) ||
      (r.full_name ? String(r.full_name).split(/\s+/)[0] : null) ||
      'Cześć';

    try {
      await sendTemplate({
        to: r.to,
        templateName: templateName,
        components: [
          {
            type: 'body',
            parameters: [{ type: 'text', text: firstName }]
          }
        ],
        phoneNumberId: phoneNumberIdOverride
      });

      console.log(`[BROADCAST] absence_reminder → ${r.to} (${firstName})`);
      await pause(BROADCAST_BATCH_SLEEP_MS);
    } catch (e) {
      console.error(`[BROADCAST ERROR] absence_reminder → ${r.to}`, e.message);
    }
  }
}

async function broadcastFreeSlotsTemplateThenList(client, phoneNumberIdOverride = null) {
  const recipients = await getAllRecipients(client);
  if (!recipients.length) {
    console.log('[BROADCAST] free-slots (tmpl): brak odbiorców');
    return;
  }
  if (!TEMPLATE_WEEKLY_SLOTS_INTRO) {
    console.warn('[BROADCAST] brak TEMPLATE_WEEKLY_SLOTS_INTRO w ENV');
    return;
  }

  // Zakres do {{1}} (opcjonalna zmienna szablonu)
  const { startYMD, endYMD } = getNextWeekRangeWarsaw();
  const rangeHuman = (() => {
    const [ys, ms, ds] = startYMD.split('-');
    const [ye, me, de] = endYMD.split('-');
    const endDate = new Date(Number(ye), Number(me)-1, Number(de));
    endDate.setDate(endDate.getDate() - 1);
    const dd = String(endDate.getDate()).padStart(2,'0');
    const mm = String(endDate.getMonth()+1).padStart(2,'0');
    const yyyy = endDate.getFullYear();
    return ` (${ds}.${ms}–${dd}.${mm}.${yyyy})`;
  })();

  const slots = await getOpenSlotsNextWeek(client);
  const listBody = formatFreeSlotsMessage(slots);

  console.log(`[BROADCAST] free-slots (tmpl+list) → ${recipients.length} osób, slots=${slots.length}`);

  for (const r of recipients) {
    // 1) szablon – otwiera okno BI 24h
    await sendTemplate({
      to: r.to,
      templateName: TEMPLATE_WEEKLY_SLOTS_INTRO,
      components: [
        { type:'body', parameters:[ { type:'text', text: rangeHuman } ] } // usuń, jeśli szablon bez zmiennych
      ],
      phoneNumberId: phoneNumberIdOverride
    });
    await pause(80);

    // 2) free-form z listą wolnych slotów (już w otwartym oknie)
    await sendText({
      to: r.to,
      body: listBody,
      phoneNumberId: phoneNumberIdOverride
    });

    await pause(BROADCAST_BATCH_SLEEP_MS);
  }
}

if (ENABLE_CRON_BROADCAST) {
  // ✅ Sobota 16:00 – prośba o zgłoszenie nieobecności
  cron.schedule('0 16 * * 6', async () => {
    const client = await pool.connect();
    try {
      console.log('[CRON] sobota 16:00 → ask-absences (template)');
      await broadcastAskAbsencesTemplate(client, null);
    } catch (e) {
      console.error('[CRON ask-absences tmpl ERROR]', e);
    } finally {
      client.release();
    }
  }, { timezone: CRON_TZ });
}

if (String(process.env.ENABLE_CRON_SLOTS || 'true').toLowerCase() === 'true') {
  // Codziennie 06:00 (Europe/Warsaw): utrzymuj horyzont 14 dni do przodu
  cron.schedule('0 6 * * *', async () => {
    const client = await pool.connect();
    try {
      console.log('[CRON] daily 06:00 → create slots rolling horizon 14d');
      const r = await createSlotsForRollingHorizon(client, 14);
      console.log(`[CRON] rolling slots: +${r.inserted} (horizon=${r.daysAhead}d)`);
    } catch (e) {
      console.error('[CRON slots ERROR]', e);
    } finally {
      client.release();
    }
  }, { timezone: CRON_TZ });
}

if (String(process.env.ENABLE_CRON_CLEANUP_SLOTS || 'true').toLowerCase() === 'true') {
  // Codziennie 03:10 (Europe/Warsaw): czyść stare open sloty
  cron.schedule('10 3 * * *', async () => {
    const client = await pool.connect();
    try {
      const retain = Math.max(1, Number(process.env.RETAIN_DAYS_OPEN_SLOTS || 60));
      console.log(`[CRON] 03:10 → cleanup open slots older than ${retain}d`);
      const r = await cleanupOpenSlots(client, retain);
      console.log(`[CRON] cleanup done: deleted=${r.deleted}`);
    } catch (e) {
      console.error('[CRON cleanup ERROR]', e);
    } finally {
      client.release();
    }
  }, { timezone: CRON_TZ });
}

/* =========================
   START
   ========================= */
const PORT = process.env.PORT || 3000;

// proste zdrowie usługi (Render sprawdza to cyklicznie)
app.get('/health', (req, res) => res.status(200).send('ok'));

// ważne: jawnie nasłuchuj na 0.0.0.0 (zewnętrzny interfejs kontenera)
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Webhook listening on :${PORT}`);
});
