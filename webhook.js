// ==== MINIMAL WHATSAPP WEBHOOK (CLEAN) ====
// Version: 2025-11-06
// Features:
//  - GET /webhook (Meta verification via VERIFY_TOKEN)
//  - POST /webhook (HMAC X-Hub-Signature-256 via APP_SECRET)
//  - Auto-reply "Dziękujemy za informację." to every inbound message
//  - Optional DB audit (safe, best-effort) to table `inbox_messages`
// Notes:
//  - No business logic, no cron, no parsers
//  - Start command: node webhook.min.js

'use strict';

const express = require('express');
const crypto = require('crypto');
const fetch = require('node-fetch');
const { Pool } = require('pg');

// ---- ENV ----
const {
  PORT = 3000,
  NODE_ENV = 'production',
  VERIFY_TOKEN,
  APP_SECRET,
  WA_TOKEN,
  WA_PHONE_ID,
  DATABASE_URL,
  LOG_LEVEL = 'info',
} = process.env;

// ---- Logger ----
function log(level, msg, obj) {
  const levels = ['error', 'warn', 'info', 'debug'];
  if (levels.indexOf(level) <= levels.indexOf(LOG_LEVEL)) {
    const line = obj ? `${msg} ${JSON.stringify(obj)}` : msg;
    console[level](line);
  }
}

// ---- DB (optional) ----
let pool = null;
if (DATABASE_URL) {
  pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  });
}

// Ensure minimal inbox table (idempotent)
async function ensureInboxSchema() {
  if (!pool) return;
  const ddl = `
    create table if not exists inbox_messages (
      id bigserial primary key,
      message_id   text,
      direction    text,
      from_phone   text,
      to_phone     text,
      message_type text,
      status       text,
      body         text,
      raw          jsonb,
      event_ts     timestamptz default now()
    );
  `;
  try {
    await pool.query(ddl);
    log('info', '[DB] inbox_messages ensured');
  } catch (e) {
    log('warn', '[DB] ensureInboxSchema failed', { error: e.message });
  }
}

async function auditInbound({ message_id, from_phone, to_phone, message_type, body, raw }) {
  if (!pool) return;
  try {
    const sql = `insert into inbox_messages
      (message_id, direction, from_phone, to_phone, message_type, body, raw)
      values ($1, 'inbound', $2, $3, $4, $5, $6)`;
    const params = [message_id, from_phone, to_phone, message_type, body, JSON.stringify(raw || null)];
    await pool.query(sql, params);
  } catch (e) {
    log('warn', '[AUDIT][INBOUND] skip', { error: e.message });
  }
}

// ---- App ----
const app = express();
app.use(express.json({
  verify: (req, _res, buf) => { req.rawBody = buf; }
}));

// HMAC verify
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

// Health
app.get('/health', (_req, res) => res.status(200).json({ ok: true, file: __filename }));

// GET verify
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

// POST receiver
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
        const toPhone = data?.metadata?.display_phone_number || null;

        if (Array.isArray(data.messages)) {
          for (const m of data.messages) {
            // audit best-effort
            await auditInbound({
              message_id: m.id,
              from_phone: m.from || null,
              to_phone: toPhone,
              message_type: m.type || null,
              body: m.text?.body ||
                    m.button?.text ||
                    m.interactive?.button_reply?.title ||
                    m.interactive?.list_reply?.title ||
                    null,
              raw: { entry, change, message: m },
            });

            // AUTO-REPLY (session message < 24h)
            if (m.from) {
              try {
                await sendText({ to: m.from, body: 'Dziękujemy za informację.' });
              } catch (e) {
                log('warn', '[OUTBOUND][ACK] failed', { error: e.message });
              }
            }
          }
        }
      }
    }
  } catch (e) {
    log('error', '[WEBHOOK][POST] Error', { error: e.message });
  }
});

// ---- Outbound helpers ----
async function sendText({ to, body, phoneNumberId = null }) {
  const phoneId = phoneNumberId || WA_PHONE_ID;
  if (!WA_TOKEN || !phoneId) throw new Error('Missing WA_TOKEN or WA_PHONE_ID');
  const url = `https://graph.facebook.com/v20.0/${phoneId}/messages`;
  const payload = { messaging_product: 'whatsapp', to, type: 'text', text: { body } };
  const res = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${WA_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`HTTP ${res.status} ${JSON.stringify(json)}`);
  log('info', '[OUTBOUND] sent text', json);
  return json;
}

// ---- Boot ----
(async () => {
  if (pool) {
    try {
      await pool.query('select 1');
      log('info', '[DB] Connected');
      await ensureInboxSchema();
    } catch (e) {
      log('warn', '[DB] connection failed', { error: e.message });
    }
  }
  app.listen(PORT, () => {
    log('info', `[BOOT] MIN webhook listening on :${PORT} (file: ${__filename})`);
  });
})();

module.exports = app;
