require('dotenv').config();
const express = require('express');
const crypto = require('crypto');
const { Pool } = require('pg');
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));

const app = express();
app.use((req, res, next) => {
  let data = [];
  req.on('data', chunk => data.push(chunk));
  req.on('end', () => { req.rawBody = Buffer.concat(data); next(); });
});
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
    `SELECT id, first_name AS name, active FROM public.users 
     WHERE phone_e164=$1 OR phone_raw=$2 OR phone_raw=$1 LIMIT 1`,
    [waPlus, waBare]
  );
  if (u.rowCount > 0) return { type: 'user', ...u.rows[0] };

  const i = await client.query(
    `SELECT id, first_name AS name FROM public.instructors 
     WHERE phone_e164=$1 OR phone_raw=$2 OR phone_raw=$1 LIMIT 1`,
    [waPlus, waBare]
  );
  if (i.rowCount > 0) return { type: 'instructor', ...i.rows[0], active: true };

  return { type: 'none' };
}

// =========================
// WHATSAPP WEBHOOK
// =========================
app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];
  if (mode === 'subscribe' && token === VERIFY_TOKEN) return res.status(200).send(challenge);
  return res.sendStatus(403);
});

app.post('/webhook', async (req, res) => {
  console.log('[WEBHOOK] incoming hit');
  console.log('[WEBHOOK BODY]', JSON.stringify(req.body).slice(0, 500));
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
              body: 'Ten numer nie jest przypisany do żadnego użytkownika. Prosimy o bezpośredni kontakt ze Studiem przez formularz kontaktowy na stronie ...'
            });
          } else if (sender.type === 'instructor') {
            // brak akcji
          } else if (sender.type === 'user' && !sender.active) {
            await sendText({ to: m.from, body: 'Dziękujemy za wiadomość.' });
          } else if (sender.type === 'user' && sender.active) {
            await sendText({ to: m.from, body: `Cześć ${sender.name}!` });
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