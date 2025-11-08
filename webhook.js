require('dotenv').config();
const express = require('express');
const crypto = require('crypto');
const { Pool } = require('pg');
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));

const app = express();

app.use(express.json({ verify: (req, res, buf) => { if (!req.rawBody) req.rawBody = Buffer.from(buf); } }));

// =========================
// ENV / KONFIG
// =========================
const VERIFY_TOKEN = process.env.VERIFY_TOKEN;
const APP_SECRET = process.env.APP_SECRET || '';
const WA_TOKEN = process.env.WHATSAPP_TOKEN || null;
const WA_PHONE_ID = process.env.WHATSAPP_PHONE_NUMBER_ID || null;
const DEBUG = String(process.env.DEBUG || '0') === '1';

const pool = new Pool(
  process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } }
    : {
        user: process.env.DB_USER || 'booking_user',
        host: process.env.DB_HOST || 'localhost',
        database: process.env.DB_NAME || 'Booking',
        password: process.env.DB_PASSWORD || '',
        port: Number(process.env.DB_PORT || 5432),
      }
);

// =========================
// UTILS
// =========================
function normalizeTo(to) { return String(to).replace(/^\+/, ''); }
async function postWA({ phoneId, payload }) {
  const url = `https://graph.facebook.com/v20.0/${phoneId}/messages`;
  const headers = { Authorization: `Bearer ${WA_TOKEN}`, 'Content-Type': 'application/json' };
  const resp = await fetch(url, { method: 'POST', headers, body: JSON.stringify(payload) });
  return { ok: resp.ok, status: resp.status, data: await resp.json().catch(() => ({})) };
}

async function sendText({ to, body, userId = null }) {
  const toNorm = normalizeTo(to);

  if (!WA_TOKEN || !WA_PHONE_ID) {
    await auditOutbound({
      userId,
      to: toNorm,
      body,
      messageType: 'text',
      status: 'skipped',
      reason: 'missing_config'
    });
    return { ok: false, reason: 'missing_config' };
  }

  const payload = {
    messaging_product: 'whatsapp',
    to: toNorm,
    type: 'text',
    text: { body }
  };

  const res = await postWA({ phoneId: WA_PHONE_ID, payload });

  if (res.ok) {
    const waMessageId = res.data?.messages?.[0]?.id || null;
    await auditOutbound({
      userId,
      to: toNorm,
      body,
      messageType: 'text',
      status: 'sent',
      waMessageId
    });
  } else {
    const reason = res.status ? `http_${res.status}` : 'send_failed';
    await auditOutbound({
      userId,
      to: toNorm,
      body,
      messageType: 'text',
      status: 'error',
      reason
    });
  }

  return res;
}

// =========================
// DB HELPERS
// =========================

//funkcja do logowania przychodzących wiadomości w pliku Inbox
async function insertInboxRecord(client, rec) {
  const sql = `INSERT INTO inbox_messages
    (source, provider_uid, message_direction, message_type, from_wa_id, text_body, sent_ts, payload_json)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT DO NOTHING;`;
  await client.query(sql, [
    rec.source, rec.provider_uid, rec.message_direction, rec.message_type,
    rec.from_wa_id, rec.text_body, rec.sent_ts, rec.payload_json
  ]);
}

// funkcja do logowania wychodzących wiadomości w tablicy outbound
async function auditOutbound({ userId, to, body, messageType, status, reason = null, waMessageId = null }) {
  const client = await pool.connect();
  try {
    await client.query(
      `
      INSERT INTO public.outbound_messages (
        user_id,
        to_phone,
        message_type,
        body,
        template_name,
        variables,
        status,
        reason,
        wa_message_id
      )
      VALUES ($1, $2, $3, $4, NULL, NULL, $5, $6, $7)
      `,
      [userId, to, messageType, body, status, reason, waMessageId]
    );
  } finally {
    client.release();
  }
}

// --- helper: identyfikacja nadawcy ---
async function resolveSenderType(client, wa) {
  if (!wa) return { type: 'none' };
  const waBare = String(wa).replace(/^\+?/, '');
  const waPlus = '+' + waBare;

  const u = await client.query(
      `SELECT id, first_name AS name, is_active AS active
      FROM public.users
      WHERE phone_e164 = $1 OR phone_raw = $2 OR phone_raw = $1
      LIMIT 1`,
    [waPlus, waBare]
  );
  if (u.rowCount > 0) return { type: 'user', ...u.rows[0] };

  const i = await client.query(
      `SELECT id, first_name AS name, is_active AS active
      FROM public.instructors
      WHERE phone_e164 = $1 OR phone_raw = $2 OR phone_raw = $1
      LIMIT 1`,
    [waPlus, waBare]
  );
  if (i.rowCount > 0) return { type: 'instructor', ...i.rows[0], active: true };

  return { type: 'none' };
}

// =========================
// Czy user ma zajęcia tego dnia? (enrollments -> class_templates.weekday_iso)
// =========================
async function userHasClassThatDay(client, userId, ymd) {
  // ymd: 'YYYY-MM-DD'
  const q = await client.query(`
    SELECT 1
    FROM public.enrollments e
    JOIN public.class_templates ct ON ct.id = e.class_template_id
    WHERE e.user_id = $1
      AND ct.weekday_iso = EXTRACT(ISODOW FROM $2::date)::int
    LIMIT 1
  `, [userId, ymd]);
  return q.rowCount > 0;
}

// =========================
// Lista zajęć usera w najbliższych 14 dniach (do menu)
// =========================
async function getUpcomingUserClasses(client, userId) {
  const sql = `
    SELECT
      d::date              AS session_date,
      ct.id                AS class_template_id,
      g.name               AS group_name,
      ct.start_time,
      COALESCE(l.name, '') AS location_name
    FROM generate_series(current_date,
                         current_date + interval '13 days',
                         interval '1 day') AS d
    JOIN public.enrollments e
      ON e.user_id = $1
    JOIN public.class_templates ct
      ON ct.id = e.class_template_id
     AND ct.is_active = true
    JOIN public.groups g
      ON g.id = ct.group_id
     AND g.is_active = true
    LEFT JOIN public.locations l
      ON l.id = g.location_id
    WHERE EXTRACT(ISODOW FROM d) = ct.weekday_iso
    ORDER BY session_date, ct.start_time, class_template_id;
  `;
  const res = await client.query(sql, [userId]);
  return res.rows;
}

// =========================
// Wysyłka menu zajęć (lista z aktywnym wyborem + "Inny termin")
// =========================
async function sendUpcomingClassesMenu({ client, to, userId }) {
  const toNorm = normalizeTo(to);

  const rows = await getUpcomingUserClasses(client, userId);

  if (!rows || rows.length === 0) {
    return sendText({
      to: toNorm,
      body: 'W najbliższych 14 dniach nie masz zaplanowanych zajęć.',
      userId
    });
  }

  if (!WA_TOKEN || !WA_PHONE_ID) {
    await auditOutbound({
      userId,
      to: toNorm,
      body: 'MENU_14_DNI (brak konfiguracji WhatsApp API)',
      messageType: 'interactive_list',
      status: 'skipped',
      reason: 'missing_config'
    });
    return { ok: false, reason: 'missing_config' };
  }

  const sectionRows = rows.map((row) => {
    const rawDate = row.session_date;
    const iso = rawDate instanceof Date
      ? rawDate.toISOString().slice(0, 10)
      : String(rawDate).slice(0, 10); // 'YYYY-MM-DD'

    const [y, m, d] = iso.split('-');
    const dateLabel = `${d}.${m}`;
    const time = String(row.start_time).slice(0, 5);
    const loc = row.location_name ? ` (${row.location_name})` : '';
    const title = `${dateLabel} ${time} ${row.group_name}${loc}`;
    const id = `absence_${iso}_${row.class_template_id}`;

    return { id, title };
  })

  const payload = {
    messaging_product: 'whatsapp',
    to: toNorm,
    type: 'interactive',
    interactive: {
      type: 'list',
      header: {
        type: 'text',
        text: 'Wybierz zajęcia'
      },
      body: {
        text: 'Poniżej Twoje zajęcia w najbliższych 14 dniach. Wybierz termin, którego dotyczy zgłoszenie nieobecności lub wybierz "Inny termin".'
      },
      footer: {
        text: 'Dla "Inny termin" wpisz np. "Zwalniam 12/11".'
      },
      action: {
        button: 'Wybierz termin',
        sections: [
          {
            title: 'Twoje zajęcia',
            rows: sectionRows
          },
          {
            title: 'Inne opcje',
            rows: [
              {
                id: 'absence_other_date',
                title: 'Inny termin',
                description: 'Podam inny termin w wiadomości'
              }
            ]
          }
        ]
      }
    }
  };

  const res = await postWA({ phoneId: WA_PHONE_ID, payload });

  const bodyLog =
    'MENU_14_DNI: ' +
    sectionRows.map(r => r.title).join(' | ') +
    ' | Inny termin';

  if (res.ok) {
    const waMessageId = res.data?.messages?.[0]?.id || null;
    await auditOutbound({
      userId,
      to: toNorm,
      body: bodyLog,
      messageType: 'interactive_list',
      status: 'sent',
      waMessageId
    });
  } else {
    const reason = res.status ? `http_${res.status}` : 'send_failed';
    await auditOutbound({
      userId,
      to: toNorm,
      body: bodyLog,
      messageType: 'interactive_list',
      status: 'error',
      reason
    });
  }

  return res;
}

// =========================
// Główna operacja: zgłoszenie nieobecności + utworzenie wolnego slotu
// =========================
async function processAbsence(client, userId, ymd) {
  try {
    await client.query('BEGIN');

    // 1) znajdź class_template_id, do którego user jest przypisany w ten dzień tygodnia
    const ctRes = await client.query(
      `
      SELECT ct.id AS class_template_id
      FROM public.enrollments e
      JOIN public.class_templates ct ON ct.id = e.class_template_id
      WHERE e.user_id = $1
        AND ct.is_active = true
        AND ct.weekday_iso = EXTRACT(ISODOW FROM $2::date)::int
      LIMIT 1
      `,
      [userId, ymd]
    );

    // jeśli brak przypisania → nie ma zajęć w ten dzień
    if (ctRes.rowCount === 0) {
      await client.query('ROLLBACK');
      return { ok: false, reason: 'no_enrollment_for_weekday' };
    }

    const classTemplateId = ctRes.rows[0].class_template_id;

    // 2) wpis do absences (audit, narastająco)
    const absRes = await client.query(
      `
      INSERT INTO public.absences (
        user_id,
        class_template_id,
        session_date,
        reason,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3::date, 'whatsapp_bezposrednia_wiadomosc', now(), now())
      ON CONFLICT (user_id, session_date)
      DO UPDATE SET
        class_template_id = EXCLUDED.class_template_id,
        reason            = EXCLUDED.reason,
        updated_at        = now()
      RETURNING id
      `,
      [userId, classTemplateId, ymd]
    );

    const absenceId = absRes.rows[0].id;

    // 3) utworzenie wolnego slotu powiązanego z absencją
    await client.query(
      `
      INSERT INTO public.slots (
        class_template_id,
        session_date,
        source_absence_id,
        status,
        taken_by_user_id,
        taken_at,
        created_at,
        updated_at
      )
      VALUES (
        $1,
        $2::date,
        $3,
        'open',
        NULL,
        NULL,
        now(),
        now()
      )
      `,
      [classTemplateId, ymd, absenceId]
    );

    await client.query('COMMIT');
    return { ok: true, absence_id: absenceId, class_template_id: classTemplateId };
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[processAbsence] error', err);
    return { ok: false, reason: 'exception', error: err.message };
  }
}

// =========================
// PARSER KOMEND (Zwalniam dd/mm)
// =========================
function parseAbsenceCommand(text) {
  const m = text.trim().match(/^zwalniam\s+(\d{1,2})[./-](\d{1,2})$/i);
  if (!m) return null;

  const day = parseInt(m[1], 10);
  const month = parseInt(m[2], 10);

  const now = new Date();
  const currentMonth = now.getMonth() + 1;
  let year = now.getFullYear();
  if (month < currentMonth) year += 1; // grudzień → styczeń

  const pad = n => String(n).padStart(2, '0');
  const ymd = `${year}-${pad(month)}-${pad(day)}`;

  return { ymd };
}

// =========================
// WYSYŁKA INTERAKTYWNEJ LISTY
// =========================



app.post('/webhook', async (req, res) => {
  if (DEBUG) {
    console.log('[WEBHOOK] incoming hit');
    try {
      console.log('[WEBHOOK BODY]', JSON.stringify(req.body || {}).slice(0, 500));
    } catch (e) {
      console.log('[WEBHOOK BODY PARSE ERROR]', e?.message);
    }
  }
  if (APP_SECRET) {
    const sig = req.get('x-hub-signature-256') || '';
    const [prefix, hex] = sig.split('=');
    const expected = crypto.createHmac('sha256', APP_SECRET).update(req.rawBody || Buffer.from([])).digest('hex');
    if (prefix !== 'sha256' || !hex || !crypto.timingSafeEqual(Buffer.from(hex, 'hex'), Buffer.from(expected, 'hex')))
      return res.status(403).send('Bad signature');
  }

  const body = req.body;
  if (!body?.entry) return res.sendStatus(200);

  const client = await pool.connect();
  try {
    for (const entry of body.entry) {
      const changes = entry?.changes || [];
      for (const ch of changes) {
        const v = ch?.value || {};
        if (!Array.isArray(v.messages)) continue;

        for (const m of v.messages) {
          const rec = {
            source: 'whatsapp',
            provider_uid: m.id,
            message_direction: 'inbound',
            message_type: m.type,
            from_wa_id: m.from,
            text_body: m.text?.body || '',
            sent_ts: new Date(Number(m.timestamp) * 1000),
            payload_json: m
          };
          await insertInboxRecord(client, rec);

          const sender = await resolveSenderType(client, m.from);
            
          if (sender.type === 'none') {
            await sendText({
              to: m.from,
              body: 'Ten numer nie jest przypisany do żadnego użytkownika. Prosimy o bezpośredni kontakt ze Studiem przez formularz kontaktowy na stronie https://agnieszkapilatesklasyczny.pl/'
            });
          } else if (sender.type === 'instructor') {
            // brak akcji
          } else if (sender.type === 'user' && !sender.active) {
            await sendText({ to: m.from, body: 'Dziękujemy za wiadomość.', userId: sender.id });
          } else if (sender.type === 'user' && sender.active) {
            await sendText({ to: m.from, body: `Cześć ${sender.name}!`, userId: sender.id });
          }

          // --- rozpoznanie komendy Zwalniam dd/mm ---
          if (sender.type === 'user' && sender.active && m.text?.body) {
            const parsed = parseAbsenceCommand(m.text.body);
            if (parsed) {
              const result = await processAbsence(client, sender.id, parsed.ymd);
              if (result.ok) {
                await sendText({
                  to: m.from,
                  body: `✔️ Nieobecność ${parsed.ymd} została zgłoszona, miejsce zwolnione.`,
                  userId: sender.id
              });
              } else if (result.reason === 'no_enrollment_for_weekday') {
                await sendUpcomingClassesMenu({
                  client,
                  to: m.from,
                  userId: sender.id
              });
              } else {
                await sendText({
                  to: m.from,
                  body: `❗ Wystąpił błąd przy zgłaszaniu nieobecności (${result.reason || 'unknown'}).`,
                  userId: sender.id
                });
              }
            }
          }
        }
      }
    }
  } catch (err) {
    console.error('[webhook] error', err);
  } finally {
    client.release();
  }
  res.sendStatus(200);
});



// =========================
// HEALTH / START
// =========================
app.get('/health', (req, res) => res.status(200).send('ok'));
const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => console.log(`Webhook listening on :${PORT}`));