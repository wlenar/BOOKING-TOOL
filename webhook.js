// ==== WHATSAPP WEBHOOK — PROVIDER SCHEMA (FINAL v2) ====
// Date: 2025-11-06
'use strict';

const express = require('express');
const crypto = require('crypto');
const { Pool } = require('pg');

// ---- Fetch (node18+: global fetch; else dynamic import) ----
const fetch = globalThis.fetch || ((...args) => import('node-fetch').then(m => m.default(...args)));

// ---- ENV ----
const {
  PORT = 3000,
  NODE_ENV = 'production',
  VERIFY_TOKEN,
  APP_SECRET,
  WA_TOKEN,
  WA_PHONE_ID,          // provider_uid
  WHATSAPP_TOKEN,
  WHATSAPP_PHONE_NUMBER_ID,
  DATABASE_URL,
  LOG_LEVEL = 'info',
} = process.env;

// prefer original names if present
function getToken() { return WA_TOKEN || WHATSAPP_TOKEN; }
function getPhoneId() { return WA_PHONE_ID || WHATSAPP_PHONE_NUMBER_ID; }

// ---- Logger ----
function log(level, msg, obj) {
  const levels = ['error', 'warn', 'info', 'debug'];
  if (levels.indexOf(level) <= levels.indexOf(LOG_LEVEL)) {
    const line = obj ? `${msg} ${JSON.stringify(obj)}` : msg;
    console[level](line);
  }
}

// ---- DB ----
const pool = DATABASE_URL ? new Pool({
  connectionString: DATABASE_URL,
  ssl: NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
}) : null;

// ---- App ----
const app = express();
app.use(express.json({ verify: (req, _res, buf) => { req.rawBody = buf; } }));

// ---- HMAC verify ----
function verifyMetaSignature(req) {
  try {
    if (!APP_SECRET) return true;
    const sig = req.get('X-Hub-Signature-256');
    if (!sig || !req.rawBody) return false;
    const digest = 'sha256=' + crypto.createHmac('sha256', APP_SECRET).update(req.rawBody).digest('hex');
    return crypto.timingSafeEqual(Buffer.from(digest), Buffer.from(sig));
  } catch {
    return false;
  }
}

// ---- Health ----
app.get('/health', (_req, res) => res.status(200).json({ ok: true, file: __filename }));

// ---- GET /webhook (verification) ----
app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];
  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    log('info', '[WEBHOOK] Verified');
    return res.status(200).send(challenge);
  }
  log('warn', '[WEBHOOK] Verification failed');
  return res.sendStatus(403);
});

// ---- POST /webhook ----
app.post('/webhook', async (req, res) => {
  if (!verifyMetaSignature(req)) {
    log('warn', '[WEBHOOK] Invalid signature');
    return res.sendStatus(403);
  }
  res.status(200).send('EVENT_RECEIVED');

  try {
    const body = req.body;
    if (body.object !== 'whatsapp_business_account') return;

    for (const entry of body.entry || []) {
      for (const change of entry.changes || []) {
        const data = change.value || {};

        // Inbound messages
        if (Array.isArray(data.messages)) {
          for (const m of data.messages) {
            try {
              await insertInboundProvider({
                provider_uid: data.metadata?.phone_number_id || getPhoneId() || null,
                provider_message_id: m.id || null,
                message_type: m.type || null,
                from_wa_id: m.from || null,
                from_msisdn: normalizeMsisdn(m.from),
                to_msisdn: normalizeMsisdn(data.metadata?.display_phone_number),
                text_body: m.text?.body ||
                           m.button?.text ||
                           m.interactive?.button_reply?.title ||
                           m.interactive?.list_reply?.title ||
                           null,
                payload_json: { entry, change, message: m },
              });
            } catch (e) {
              log('warn', '[AUDIT][INBOUND] skip', { error: e.message });
            }

            // Auto-ACK
            if (m.from) {
              try {
                await sendText({ to: m.from, body: 'Dziękujemy za wiadomość.' });
              } catch (e) {
                log('warn', '[OUTBOUND][ACK] failed', { error: e.message });
              }
            }
          }
        }

        // Status updates
        if (Array.isArray(data.statuses)) {
          for (const s of data.statuses) {
            try {
              await insertStatusProvider({
                provider_uid: data.metadata?.phone_number_id || getPhoneId() || null,
                provider_message_id: s.id || null,
                to_msisdn: normalizeMsisdn(s.recipient_id),
                status: s.status || null,
                error_code: s.errors?.[0]?.code ? String(s.errors[0].code) : null,
                error_title: s.errors?.[0]?.title || null,
                sent_ts: s.timestamp ? toTimestamp(s.timestamp) : null,
                payload_json: { entry, change, status: s },
              });
            } catch (e) {
              log('warn', '[AUDIT][STATUS] skip', { error: e.message });
            }
          }
        }
      }
    }
  } catch (e) {
    log('error', '[WEBHOOK][POST] Error', { error: e.message });
  }
});

// ---- Inserts for provider schema ----
async function insertInboundProvider({
  provider_uid,
  provider_message_id,
  message_type,
  from_wa_id,
  from_msisdn,
  to_msisdn,
  text_body,
  payload_json,
}) {
  if (!pool) return;
  const sql = `
    insert into inbox_messages (source, provider_uid, provider_message_id, message_direction, message_type,
       from_wa_id, from_msisdn, to_msisdn, text_body, payload_json, received_at)
    select 'whatsapp', $1, $2, 'inbound', $3, $4, $5, $6, $7, $8, now()
    where not exists (
      select 1 from inbox_messages i
      where i.source = 'whatsapp'
        and i.provider_uid = $1
        and i.provider_message_id = $2
    )
  `;
  const params = [
    provider_uid,
    provider_message_id,
    message_type,
    from_wa_id,
    from_msisdn,
    to_msisdn,
    text_body,
    JSON.stringify(payload_json || null),
  ];
  await pool.query(sql, params);
}

async function insertStatusProvider({
  provider_uid,
  provider_message_id,
  to_msisdn,
  status,
  error_code,
  error_title,
  sent_ts,
  payload_json,
}) {
  if (!pool) return;
  const sql = `
    insert into inbox_messages (source, provider_uid, provider_message_id, message_direction, to_msisdn,
       status, error_code, error_title, sent_ts, payload_json, received_at)
    select 'whatsapp', $1, $2, 'outbound', $3, $4, $5, $6, $7, $8, now()
    where not exists (
      select 1 from inbox_messages i
      where i.source = 'whatsapp'
        and i.provider_uid = $1
        and i.provider_message_id = $2
    )
  `;
  const params = [
    provider_uid,
    provider_message_id,
    to_msisdn,
    status,
    error_code,
    error_title,
    sent_ts,
    JSON.stringify(payload_json || null),
  ];
  await pool.query(sql, params);
}

// ---- Outbound helper ----
async function sendText({ to, body, phoneNumberId = null }) {
  const phoneId = phoneNumberId || getPhoneId();
  const token = getToken();
  if (!token || !phoneId) throw new Error('Missing token or phone id');
  const url = `https://graph.facebook.com/v20.0/${phoneId}/messages`;
  const payload = {
    messaging_product: 'whatsapp',
    to,
    type: 'text',
    text: { body },
  };
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`HTTP ${res.status} ${JSON.stringify(json)}`);
  log('info', '[OUTBOUND] sent text', json);
  return json;
}

// ---- Helpers ----
function normalizeMsisdn(msisdn) {
  if (!msisdn) return null;
  if (msisdn.startsWith('+')) return msisdn;
  if (/^\d+$/.test(msisdn)) return '+' + msisdn;
  return msisdn;
}
function toTimestamp(epochSecString) {
  const n = Number(epochSecString);
  if (!Number.isFinite(n)) return null;
  return new Date(n * 1000).toISOString();
}

// ---- Boot ----
(async () => {
  if (pool) {
    try {
      await pool.query('select 1');
      log('info', '[DB] Connected');
    } catch (e) {
      log('warn', '[DB] connection failed', { error: e.message });
    }
  }
  app.listen(PORT, () => {
    log('info', `[BOOT] PROVIDER webhook listening on :${PORT} (file: ${__filename})`);
  });
})();

module.exports = app;
