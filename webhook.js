// minimal-webhook.js
require('dotenv').config();

const express = require('express');
const crypto  = require('crypto');
const { Pool } = require('pg');
// Node 18+ ma globalny fetch; dla 16+ użyj dynamicznego importu jak w Twoim pliku
const fetch = global.fetch || ((...args) => import('node-fetch').then(({default:f}) => f(...args)));

const app = express();

// --- surowe body do ewentualnej weryfikacji podpisu ---
app.use((req, res, next) => {
  const chunks = [];
  req.on('data', c => chunks.push(c));
  req.on('end', () => { req.rawBody = Buffer.concat(chunks); next(); });
});
app.use(express.json({
  verify: (req, res, buf) => { if (!req.rawBody) req.rawBody = Buffer.from(buf); }
}));

/* ========= ENV / DB ========= */
const VERIFY_TOKEN  = process.env.VERIFY_TOKEN;
const APP_SECRET    = process.env.APP_SECRET || '';
const WA_TOKEN      = process.env.WHATSAPP_TOKEN || null;
const WA_PHONE_ID   = process.env.WHATSAPP_PHONE_NUMBER_ID || null;

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

/* ========= DB helpers ========= */
async function insertInboxRecord(client, rec) {
  const sql = `
    INSERT INTO inbox_messages (
      source, provider_uid, provider_message_id, message_direction, message_type,
      from_wa_id, from_msisdn, to_msisdn, text_body, status, error_code, error_title, sent_ts, payload_json
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14
    )
    ON CONFLICT (source, provider_uid) DO NOTHING
    RETURNING id;
  `;
  const params = [
    rec.source, rec.provider_uid, rec.provider_message_id, rec.message_direction, rec.message_type,
    rec.from_wa_id, rec.from_msisdn, rec.to_msisdn, rec.text_body, rec.status, rec.error_code,
    rec.error_title, rec.sent_ts, rec.payload_json
  ];
  const r = await client.query(sql, params);
  return { inserted: r.rows.length > 0, id: r.rows[0]?.id || null };
}

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

/* ========= OUTBOUND (prosty) ========= */
async function sendText({ to, body, phoneNumberId = WA_PHONE_ID }) {
  if (!WA_TOKEN || !phoneNumberId) {
    await auditOutbound({ user_id: null, to_phone: to, message_type: 'text', body, status: 'skipped', reason: 'missing_config' });
    return { ok: false, reason: 'missing_config' };
  }
  const url = `https://graph.facebook.com/v20.0/${phoneNumberId}/messages`;
  const payload = {
    messaging_product: 'whatsapp',
    to: String(to).replace(/^\+/, ''), // WA bez plusa
    type: 'text',
    text: { body }
  };

  await auditOutbound({ user_id: null, to_phone: to, message_type: 'text', body, status: 'queued' });

  const resp = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${WA_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  const text = await resp.text().catch(() => '');
  let data; try { data = JSON.parse(text); } catch { data = { raw: text }; }

  if (resp.ok) {
    await auditOutbound({
      user_id: null, to_phone: to, message_type: 'text', body,
      status: 'sent', wa_message_id: data?.messages?.[0]?.id || null
    });
    return { ok: true, data };
  } else {
    await auditOutbound({
      user_id: null, to_phone: to, message_type: 'text', body,
      status: 'error', reason: JSON.stringify(data).slice(0,500)
    });
    return { ok: false, status: resp.status, data };
  }
}

/* ========= Mapowanie minimalne ========= */
function mapWhatsAppMessageToRecord(message, value) {
  const ts = message?.timestamp ? new Date(Number(message.timestamp) * 1000) : null;
  const displayNumber = value?.metadata?.display_phone_number || null;
  const phoneNumberId  = value?.metadata?.phone_number_id || null;
  const txt =
    message?.text?.body ||
    message?.button?.text ||
    message?.interactive?.button_reply?.title ||
    message?.interactive?.list_reply?.title ||
    null;

  return {
    source: 'whatsapp',
    provider_uid: message?.id,
    provider_message_id: message?.id,
    message_direction: 'inbound',
    message_type: message?.type || 'unknown',
    from_wa_id: message?.from || null,
    from_msisdn: message?.from ? `+${String(message.from).replace(/^\+?/, '')}` : null,
    to_msisdn: displayNumber || phoneNumberId || null,
    text_body: txt,
    status: null,
    error_code: null,
    error_title: null,
    sent_ts: ts,
    payload_json: message || {}
  };
}

/* ========= GET /webhook (verify) ========= */
app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];
  if (mode === 'subscribe' && token === VERIFY_TOKEN) return res.status(200).send(challenge);
  return res.sendStatus(403);
});

/* ========= POST /webhook ========= */
app.post('/webhook', async (req, res) => {
  // Opcjonalna weryfikacja podpisu (jeśli APP_SECRET ustawiony)
  if (APP_SECRET) {
    const hdr = req.get('x-hub-signature-256') || '';
    const [prefix, sigHex] = hdr.split('=');
    if (prefix !== 'sha256' || !sigHex) return res.status(403).send('Missing signature');
    const expected = crypto.createHmac('sha256', APP_SECRET).update(req.rawBody || Buffer.from([])).digest('hex');
    const a = Buffer.from(sigHex, 'hex'), b = Buffer.from(expected, 'hex');
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return res.status(403).send('Bad signature');
  }

  let body = req.body;
  if (!body || !Array.isArray(body.entry)) return res.sendStatus(200);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (const entry of body.entry) {
      const changes = entry?.changes || [];
      for (const ch of changes) {
        const v = ch?.value || {};
        const phoneNumberIdFromHook = v?.metadata?.phone_number_id || WA_PHONE_ID;

        if (Array.isArray(v.messages)) {
          for (const m of v.messages) {
            // 1) zapisz inbound do inbox_messages
            const rec = mapWhatsAppMessageToRecord(m, v);
            const ins = await insertInboxRecord(client, rec);

            // jeśli duplikat – nic więcej nie rób (idempotentnie)
            if (!ins.inserted) continue;

            // 2) odeślij krótkie potwierdzenie
            const to = rec.from_wa_id; // WA ID nadawcy (msisdn bez +)
            if (to) {
              await sendText({
                to,
                body: 'Dziękujemy za informację',
                phoneNumberId: phoneNumberIdFromHook
              });
              // (auditOutbound zapisuje outbound_messages)
            }
          }
        }
      }
    }

    await client.query('COMMIT');
    return res.sendStatus(200);
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[WEBHOOK ERROR]', e);
    return res.sendStatus(500);
  } finally {
    client.release();
  }
});

/* ========= HEALTH + START ========= */
app.get('/health', (req, res) => res.status(200).send('ok'));
const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => console.log(`Webhook listening on :${PORT}`));