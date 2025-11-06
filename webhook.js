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
async function sendText({ to, body }) {
  const payload = { messaging_product: 'whatsapp', to: normalizeTo(to), type: 'text', text: { body } };
  await postWA({ phoneId: WA_PHONE_ID, payload });
}

// =========================
// DB HELPERS
// =========================
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
// Główna operacja: zgłoszenie nieobecności + utworzenie wolnego slotu
// =========================
async function processAbsence(client, userId, ymd) {
  try {
    await client.query('BEGIN');

    // 1) weryfikacja czy user ma zajęcia w ten dzień (po weekday_iso)
    const has = await userHasClassThatDay(client, userId, ymd);
    if (!has) {
      await client.query('ROLLBACK');
      return { ok: false, reason: 'no_enrollment_for_weekday' };
    }

    // 2) (opcjonalnie) ustalenie właściwego class_template_id pasującego do weekday
    const ct = await client.query(`
      SELECT ct.id AS class_template_id
      FROM public.enrollments e
      JOIN public.class_templates ct ON ct.id = e.class_template_id
      WHERE e.user_id = $1
        AND ct.weekday_iso = EXTRACT(ISODOW FROM $2::date)::int
      ORDER BY ct.start_time
      LIMIT 1
    `, [userId, ymd]);

    if (ct.rowCount === 0) {
      await client.query('ROLLBACK');
      return { ok: false, reason: 'mapping_not_found' };
    }
    const classTemplateId = ct.rows[0].class_template_id;

    // 3) wpis do absences (wg schemy: user_id, session_date, created_at)
    const insAbs = await client.query(`
      INSERT INTO public.absences (user_id, session_date, created_at)
      VALUES ($1, $2::date, now())
      ON CONFLICT (user_id, session_date) DO NOTHING
      RETURNING id
    `, [userId, ymd]);

    // id absencji (jeśli ON CONFLICT – spróbujemy je pobrać)
    let absenceId = insAbs.rows[0]?.id ?? null;
    if (!absenceId) {
      const getAbs = await client.query(`
        SELECT id FROM public.absences
        WHERE user_id = $1 AND session_date = $2::date
        LIMIT 1
      `, [userId, ymd]);
      absenceId = getAbs.rows[0]?.id ?? null;
    }

    // 4) utworzenie wolnego slotu w slots
    //    (bez przypisania usera; oznacz jako open; źródło = ta absencja — jeśli masz kolumnę referencyjną)
    //    Uwaga: NIE zgaduję nazwy kolumny źródła. Jeśli masz np. slots.absence_id — dopisz w INSERT.
    await client.query(`
      INSERT INTO public.slots (class_template_id, session_date, is_open, created_at)
      VALUES ($1, $2::date, true, now())
    `, [classTemplateId, ymd]);

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
// LISTA ZAJĘĆ UŻYTKOWNIKA (14 dni)
// =========================
async function buildUpcomingClassesList(client, userId) {
  const res = await client.query(`
    SELECT e.id AS enrollment_id,
           g.name AS group_name,
           e.session_date,
           ct.start_time
      FROM public.enrollments e
      JOIN public.class_templates ct ON ct.id = e.class_template_id
      JOIN public.groups g ON g.id = ct.group_id
     WHERE e.user_id = $1
       AND e.status = 'booked'
       AND e.session_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '14 days'
     ORDER BY e.session_date, ct.start_time
     LIMIT 10;
  `, [userId]);

  return res.rows.map(r => ({
    id: `cancel_${r.enrollment_id}`,
    title: `${r.group_name}`,
    description: `${r.session_date.toISOString().slice(0,10)} ${r.start_time.slice(0,5)}`
  }));
}

// =========================
// WYSYŁKA INTERAKTYWNEJ LISTY
// =========================
async function sendAbsenceList(to, classes) {
  const payload = {
    messaging_product: 'whatsapp',
    to: String(to).replace(/^\+/, ''),
    type: 'interactive',
    interactive: {
      type: 'list',
      body: { text: 'Wybierz zajęcia, które chcesz odwołać:' },
      footer: { text: 'Jeśli chcesz zgłosić inny termin, wybierz opcję poniżej.' },
      action: {
        button: 'Wybierz',
        sections: [
          {
            title: 'Twoje zajęcia (14 dni)',
            rows: classes
          },
          {
            title: 'Inne opcje',
            rows: [{ id: 'cancel_other', title: 'Inny termin', description: 'Podaj datę ręcznie' }]
          }
        ]
      }
    }
  };
  await postWA({ phoneId: WA_PHONE_ID, payload });
}


app.post('/webhook', async (req, res) => {
  //komunikat na log czy wiadomosc zostala otrzymana
  console.log('[WEBHOOK] incoming hit');
  //komunikat na log czy wiadomoc zostala sparsowana
  try {
  console.log('[WEBHOOK BODY]', JSON.stringify(req.body || {}).slice(0, 500));
  } catch (e) {
  console.log('[WEBHOOK BODY PARSE ERROR]', e?.message);
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
          console.log('[SENDER]', sender);
            
          if (sender.type === 'none') {
            await sendText({
              to: m.from,
              body: 'Ten numer nie jest przypisany do żadnego użytkownika. Prosimy o bezpośredni kontakt ze Studiem przez formularz kontaktowy na stronie ...'
            });
          } else if (sender.type === 'instructor') {
            // brak akcji
          } else if (sender.type === 'user' && !sender.active) {
            await sendText({ to: m.from, body: 'Dziękujemy za wiadomość.' });
          } else if (sender.type === 'user' && sender.active) {
            await sendText({ to: m.from, body: `Cześć ${sender.name}!` });
          }

          // --- rozpoznanie komendy Zwalniam dd/mm ---
          if (sender.type === 'user' && sender.active && m.text?.body) {
            const parsed = parseAbsenceCommand(m.text.body);
            if (parsed) {
              const result = await processAbsence(client, sender.id, parsed.ymd);
              if (result.ok) {
                await sendText({
                  to: m.from,
                  body: `✔️ Nieobecność ${parsed.ymd} została zgłoszona, miejsce zwolnione.`
                });
              } else if (result.reason === 'no_enrollment_for_weekday') {
              const upcoming = await buildUpcomingClassesList(client, sender.id);

                if (upcoming.length > 0) {
                  await sendAbsenceList(m.from, upcoming);
                } else {
                  await sendText({
                    to: m.from,
                    body: '❗ Nie masz żadnych zajęć w najbliższych 14 dniach. Jeśli chcesz zgłosić inny termin, napisz np. "Zwalniam 05/12".'
                  });
                }
              } else {
                await sendText({
                  to: m.from,
                  body: `❗ Wystąpił błąd przy zgłaszaniu nieobecności (${result.reason || 'unknown'}).`
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