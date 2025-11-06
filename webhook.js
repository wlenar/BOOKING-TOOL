// Clean WhatsApp Webhook — minimal, production-ready skeleton
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

// ---- DB ----
const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
    })
  : null;

// Minimal DB ping at startup (non-fatal if missing DB)
(async () => {
  if (!pool) return;
  try {
    await pool.query('select 1');
    log('info', '[DB] Connected');
  } catch (err) {
    log('error', '[DB] Connection failed', err.message);
  }
})();

// ---- App ----
const app = express();

// Capture raw body for HMAC verification
app.use(
  express.json({
    verify: (req, _res, buf) => {
      req.rawBody = buf;
    },
  })
);

// ---- Utils ----
function log(level, msg, obj) {
  const levels = ['error', 'warn', 'info', 'debug'];
  if (levels.indexOf(level) <= levels.indexOf(LOG_LEVEL)) {
    const line = obj ? `${msg} ${JSON.stringify(obj)}` : msg;
    // eslint-disable-next-line no-console
    console[level](line);
  }
}

// Meta HMAC verification (X-Hub-Signature-256)
function verifyMetaSignature(req) {
  try {
    if (!APP_SECRET) return true; // allow if not configured
    const sig = req.get('X-Hub-Signature-256');
    if (!sig || !req.rawBody) return false;
    const hmac = crypto.createHmac('sha256', APP_SECRET);
    const digest = 'sha256=' + hmac.update(req.rawBody).digest('hex');
    return crypto.timingSafeEqual(Buffer.from(digest), Buffer.from(sig));
  } catch {
    return false;
  }
}

// ---- Health ----
app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true, service: 'whatsapp-webhook', env: NODE_ENV });
});

// ---- Webhook verification (GET) ----
app.get('/webhook', (req, res) => {
  try {
    const mode = req.query['hub.mode'];
    const token = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];

    if (mode === 'subscribe' && token === VERIFY_TOKEN) {
      log('info', '[WEBHOOK] Verified');
      return res.status(200).send(challenge);
    }
    log('warn', '[WEBHOOK] Verification failed');
    return res.sendStatus(403);
  } catch (err) {
    log('error', '[WEBHOOK][GET] Error', { error: err.message });
    return res.sendStatus(500);
  }
});

// ---- Webhook receiver (POST) ----
app.post('/webhook', async (req, res) => {
  if (!verifyMetaSignature(req)) {
    log('warn', '[WEBHOOK] Invalid signature');
    return res.sendStatus(403);
  }

  // Acknowledge immediately (Meta requires <10s)
  res.status(200).send('EVENT_RECEIVED');

  try {
    const body = req.body;
    if (body.object !== 'whatsapp_business_account') return;

    for (const entry of body.entry || []) {
      for (const change of entry.changes || []) {
        const data = change.value || {};

        // Incoming messages
        if (Array.isArray(data.messages)) {
          for (const m of data.messages) {
            try {
              await auditInbound({
                platform: 'whatsapp',
                message_id: m.id,
                from_phone: m.from,
                to_phone: data.metadata?.display_phone_number || null,
                message_type: m.type,
                body:
                  m.text?.body ||
                  m.button?.text ||
                  m.interactive?.button_reply?.title ||
                  m.interactive?.list_reply?.title ||
                  null,
                raw: { entry, change, message: m },
              });

              // --- Simple auto-reply ---
              // Send a short acknowledgement to the sender (24h session message)
              if (m.from) {
                await sendText({ to: m.from, body: 'Dziękujemy za informację.' });
              }
            } catch (e) {
              log('error', '[INBOUND] Handling failed', { error: e.message });
            }
          }
        }// Delivery/read statuses for outbound messages
        if (Array.isArray(data.statuses)) {
          for (const s of data.statuses) {
            try {
              await auditStatus({
                platform: 'whatsapp',
                message_id: s.id,
                to_phone: s.recipient_id || null,
                status: s.status, // sent, delivered, read, failed, etc.
                timestamp: s.timestamp ? Number(s.timestamp) : null,
                raw: { entry, change, status: s },
              });
            } catch (e) {
              log('error', '[AUDIT][STATUS] Failed', { error: e.message });
            }
          }
        }
      }
    }
  } catch (err) {
    log('error', '[WEBHOOK][POST] Error', { error: err.message });
  }
});

// ---- Minimal send helpers ----
async function sendText({ to, body, phoneNumberId = null }) {
  const phoneId = phoneNumberId || WA_PHONE_ID;
  if (!WA_TOKEN || !phoneId) {
    log('warn', '[OUTBOUND] Skipped send (missing WA_TOKEN or WA_PHONE_ID)');
    return { ok: false, reason: 'missing_config' };
  }
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
      Authorization: `Bearer ${WA_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    log('error', '[OUTBOUND] Send failed', { status: res.status, json });
    return { ok: false, status: res.status, json };
  }
  log('info', '[OUTBOUND] Sent text', json);
  return { ok: true, json };
}

async function sendTemplate({ to, templateName, language = 'pl', components = [], phoneNumberId = null }) {
  const phoneId = phoneNumberId || WA_PHONE_ID;
  if (!WA_TOKEN || !phoneId) {
    log('warn', '[OUTBOUND] Skipped template (missing WA_TOKEN or WA_PHONE_ID)');
    return { ok: false, reason: 'missing_config' };
    }
  const url = `https://graph.facebook.com/v20.0/${phoneId}/messages`;
  const payload = {
    messaging_product: 'whatsapp',
    to,
    type: 'template',
    template: {
      name: templateName,
      language: { code: language },
      components,
    },
  };
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${WA_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    log('error', '[OUTBOUND] Template failed', { status: res.status, json });
    return { ok: false, status: res.status, json };
  }
  log('info', '[OUTBOUND] Sent template', json);
  return { ok: true, json };
}

// ---- Auditing (DB) ----
async function auditInbound({
  platform,
  message_id,
  from_phone,
  to_phone,
  message_type,
  body,
  raw,
}) {
  if (!pool) return;
  const sql = `
    insert into inbox_messages
      (platform, message_id, direction, from_phone, to_phone, message_type, body, raw)
    values
      ($1, $2, 'inbound', $3, $4, $5, $6, $7)
  `;
  const params = [platform, message_id, from_phone, to_phone, message_type, body, JSON.stringify(raw || null)];
  await pool.query(sql, params);
}

async function auditStatus({
  platform,
  message_id,
  to_phone,
  status,
  timestamp,
  raw,
}) {
  if (!pool) return;
  const sql = `
    insert into inbox_messages
      (platform, message_id, direction, to_phone, status, body, raw)
    values
      ($1, $2, 'status', $3, $4, $5, $6)
  `;
  const body = timestamp ? `ts:${timestamp}` : null;
  const params = [platform, message_id, to_phone, status, body, JSON.stringify(raw || null)];
  await pool.query(sql, params);
}

// ---- Start ----
app.listen(PORT, () => {
  log('info', `[BOOT] Webhook listening on :${PORT}`);
});

module.exports = app;
