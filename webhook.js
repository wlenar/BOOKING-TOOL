require('dotenv').config();
const express = require('express');
const crypto = require('crypto');
const { Pool } = require('pg');
const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));
const cron = require('node-cron');

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
async function auditOutbound({
  userId,
  to,
  body,
  messageType,
  status,
  reason = null,
  waMessageId = null,
  templateName = null,
  variables = null
}) {
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
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `,
      [
        userId,
        to,
        messageType,
        body,
        templateName,
        variables,
        status,
        reason,
        waMessageId
      ]
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
  if (i.rowCount > 0) return { type: 'instructor', ...i.rows[0] };

  return { type: 'none' };
}

// =========================
// LISTA ZAJĘĆ UŻYTKOWNIKA (14 dni) + MENU ABSENCJI
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

  const sectionRows = rows
    .slice(0, 10) // WA: max 10 wierszy w sekcji
    .map((row) => {
      const rawDate = row.session_date;
      const iso = rawDate instanceof Date
        ? rawDate.toISOString().slice(0, 10)
        : String(rawDate).slice(0, 10); // YYYY-MM-DD

      const [y, m, d] = iso.split('-');
      const yy = y.slice(2, 4);

      let title = `${d}/${m}/${yy} ${row.group_name}`;
      if (title.length > 24) {
        title = title.slice(0, 24); // twardy limit WA
      }

      const id = `absence_${iso}_${row.class_template_id}`;
      return { id, title };
    });

  const payload = {
    messaging_product: 'whatsapp',
    to: toNorm,
    type: 'interactive',
    interactive: {
      type: 'list',
      body: {
        text: 'Wybierz termin zajęć, dla których chcesz zgłosić nieobecność, lub wybierz "Inny termin".'
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

async function sendAbsenceMoreQuestion({ to, userId }) {
  const toNorm = normalizeTo(to);

  if (!WA_TOKEN || !WA_PHONE_ID) {
    await auditOutbound({
      userId,
      to: toNorm,
      body: 'ABSENCE_MORE (brak konfiguracji WhatsApp API)',
      messageType: 'interactive_buttons',
      status: 'skipped',
      reason: 'missing_config'
    });
    return { ok: false, reason: 'missing_config' };
  }

  const payload = {
    messaging_product: 'whatsapp',
    to: toNorm,
    type: 'interactive',
    interactive: {
      type: 'button',
      body: {
        text: 'Czy chcesz zgłosić kolejną nieobecność?'
      },
      action: {
        buttons: [
          {
            type: 'reply',
            reply: { id: 'absence_more_yes', title: '✅ Tak' }
          },
          {
            type: 'reply',
            reply: { id: 'absence_more_no', title: '❌ Nie' }
          }
        ]
      }
    }
  };

  const res = await postWA({ phoneId: WA_PHONE_ID, payload });

  const bodyLog = 'ABSENCE_MORE: [Tak] [Nie]';

  if (res.ok) {
    const waMessageId = res.data?.messages?.[0]?.id || null;
    await auditOutbound({
      userId,
      to: toNorm,
      body: bodyLog,
      messageType: 'interactive_buttons',
      status: 'sent',
      waMessageId
    });
  } else {
    const reason = res.status ? `http_${res.status}` : 'send_failed';
    await auditOutbound({
      userId,
      to: toNorm,
      body: bodyLog,
      messageType: 'interactive_buttons',
      status: 'error',
      reason
    });
  }

  return res;
}

async function sendMakeupMenu({ client, to, userId }) {
  const toNorm = normalizeTo(to);


  // pobierz dostępne oferty dla tego usera
  const q = await client.query(
    `
    SELECT
      so.id              AS offer_id,
      os.slot_id,
      os.session_date,
      os.session_time,
      os.group_name
    FROM public.slot_offers so
    JOIN public.v_open_slots_desc os
      ON os.slot_id = so.slot_id
    WHERE
          so.user_id = $1
      AND so.status IN ('pending', 'sent')
      AND os.session_date >= current_date
      AND os.session_date <  current_date + interval '7 days'
      AND os.free_capacity_remaining > 0
    ORDER BY os.session_date, os.session_time, os.group_name
    LIMIT 10
    `,
    [userId]
  );

  const rows = q.rows || [];

  if (rows.length === 0) {
    return sendText({
      to: toNorm,
      body: 'Aktualnie nie ma dostępnych wolnych miejsc do odrabiania w najbliższym tygodniu.',
      userId
    });
  }

  if (!WA_TOKEN || !WA_PHONE_ID) {
    await auditOutbound({
      userId,
      to: toNorm,
      body: 'MAKEUP_MENU (brak konfiguracji WhatsApp API)',
      messageType: 'interactive_list',
      status: 'skipped',
      reason: 'missing_config'
    });
    return { ok: false, reason: 'missing_config' };
  }

  const listRows = rows.map((r) => {
    const d = r.session_date;
    const iso = d instanceof Date
      ? d.toISOString().slice(0, 10)
      : String(d).slice(0, 10);

    const [y, m, dd] = iso.split('-');
    const dateLabel = `${dd}/${m}`;
    const timeLabel = (r.session_time || '').toString().slice(0, 5);

    const title = `${dateLabel} ${timeLabel}`.trim();
    const desc = r.group_name || '';

    return {
      id: `makeup_${r.offer_id}`,
      title: title.substring(0, 24),
      ...(desc ? { description: desc.substring(0, 80) } : {})
    };
  });

  const payload = {
    messaging_product: 'whatsapp',
    to: toNorm,
    type: 'interactive',
    interactive: {
      type: 'list',
      body: {
        text: 'Wybierz termin z dostępnych wolnych miejsc do odrabiania.'
      },
      action: {
        button: 'Wybierz termin',
        sections: [
          {
            title: 'Dostępne miejsca',
            rows: listRows
          }
        ]
      }
    }
  };

  const res = await postWA({ phoneId: WA_PHONE_ID, payload });

  const bodyLog =
    'MAKEUP_MENU: ' + listRows.map(r => r.title).join(' | ');

  const waMessageId = res.data?.messages?.[0]?.id || null;
  const status = res.ok ? 'sent' : 'error';
  const reason = res.ok
    ? null
    : (res.status ? `http_${res.status}` : 'send_failed');

  await auditOutbound({
    userId,
    to: toNorm,
    body: bodyLog,
    messageType: 'interactive_list',
    status,
    reason,
    waMessageId
  });

  return res;
}

async function sendMainMenu({ to, userId }) {
  const toNorm = normalizeTo(to);

  if (!WA_TOKEN || !WA_PHONE_ID) {
    const body =
      'MENU GŁÓWNE:\n' +
      '1. Zgłoszenie nieobecności\n' +
      '2. Odrabianie zajęć\n' +
      '3. Ilość nieobecności\n' +
      '4. Zakończ rozmowę';
    await auditOutbound({
      userId,
      to: toNorm,
      body,
      messageType: 'text',
      status: 'skipped',
      reason: 'missing_config'
    });
    return;
  }

  const payload = {
    messaging_product: 'whatsapp',
    to: toNorm,
    type: 'interactive',
    interactive: {
      type: 'list',
      body: {
        text:
          'Wybierz jedną z opcji:\n' +
          '1. Zgłoszenie nieobecności\n' +
          '2. Odrabianie zajęć\n' +
          '3. Ilość nieobecności\n' +
          '4. Zakończ rozmowę'
      },
      action: {
        button: 'Menu główne',
        sections: [
          {
            title: 'Menu główne',
            rows: [
              { id: 'menu_absence', title: 'Zgłoszenie nieobecności' },
              { id: 'menu_makeup', title: 'Odrabianie zajęć' },
              { id: 'menu_credits', title: 'Ilość nieobecności' },
              { id: 'menu_end', title: 'Zakończ rozmowę' }
            ]
          }
        ]
      }
    }
  };

  const res = await postWA({ phoneId: WA_PHONE_ID, payload });

  const bodyLog =
    'MENU_GLOWNE: 1-Zgłoszenie nieobecności | 2-Odrabianie zajęć | 3-Ilość nieobecności | 4-Zakończ rozmowę';

  const waMessageId = res.data?.messages?.[0]?.id || null;
  const status = res.ok ? 'sent' : 'error';
  const reason = res.ok
    ? null
    : (res.status ? `http_${res.status}` : 'send_failed');

  await auditOutbound({
    userId,
    to: toNorm,
    body: bodyLog,
    messageType: 'interactive_list',
    status,
    reason,
    waMessageId
  });

  return res;
}

async function runWeeklySlotsJob() {
  const client = await pool.connect();
  try {
    console.log('[CRON] job_weekly_slots start');
    await client.query('SELECT public.job_weekly_slots();');
    console.log('[CRON] job_weekly_slots done');
  } catch (err) {
    console.error('[CRON] job_weekly_slots error', err);
  } finally {
    client.release();
  }
}

async function runWeeklySlotsBroadcast() {
  if (!WA_TOKEN || !WA_PHONE_ID) {
    console.log('[CRON] weekly_slots_broadcast skipped (missing WA config)');
    return;
  }

  const client = await pool.connect();
  try {
    console.log('[CRON] weekly_slots_broadcast start');

    // 1) przygotuj oferty w bazie
    await client.query('SELECT public.job_prepare_weekly_slot_offers();');

    // 2) pending oferty
    const { rows } = await client.query(`
      SELECT
        so.id AS offer_id,
        so.user_id,
        u.first_name,
        COALESCE(
          u.phone_e164,
          CASE
            WHEN u.phone_raw IS NOT NULL
            THEN '+' || REGEXP_REPLACE(u.phone_raw, '^\\+?', '')
            ELSE NULL
          END
        ) AS phone,
        os.slot_id,
        to_char(os.session_date, 'DD.MM')   AS session_date_label,
        to_char(os.session_time, 'HH24:MI') AS session_time_label,
        os.group_name
      FROM public.slot_offers so
      JOIN public.users u
        ON u.id = so.user_id
       AND u.is_active = true
      JOIN public.v_open_slots_desc os
        ON os.slot_id = so.slot_id
      WHERE
            so.status = 'pending'
        AND os.session_date >= current_date
        AND os.session_date <  current_date + interval '7 days'
        AND os.free_capacity_remaining > 0
      ORDER BY so.user_id, os.session_date, os.session_time, os.group_name
    `);

    if (!rows.length) {
      console.log('[CRON] weekly_slots_broadcast no pending offers');
      return;
    }

    // 3) grupowanie po user_id
    const byUser = new Map();
    for (const r of rows) {
      if (!r.phone) continue;
      if (!byUser.has(r.user_id)) {
        byUser.set(r.user_id, {
          userId: r.user_id,
          firstName: r.first_name || '',
          phone: r.phone,
          offers: []
        });
      }
      byUser.get(r.user_id).offers.push(r);
    }

    // 4) wysyłka per użytkownik
    for (const u of byUser.values()) {
      const toNorm = normalizeTo(u.phone);
      if (!toNorm) continue;

      const introBody =
        '🌿 Dostępne miejsca do odrabiania w tym tygodniu:\n' +
        u.offers
          .slice(0, 10)
          .map(o => `${o.session_date_label} ${o.session_time_label} ${o.group_name}`)
          .join('\n') +
        '\n\nWybierz termin z listy, aby go zarezerwować.';

      await sendText({
        to: toNorm,
        body: introBody,
        userId: u.userId
      });

      const listOffers = u.offers.slice(0, 10);
      if (!listOffers.length) continue;

      const rowsList = listOffers.map(o => ({
        id: `makeup_${o.offer_id}`,
        title: `${o.session_date_label} ${o.session_time_label}`.substring(0, 24),
        description: (o.group_name || '').substring(0, 80)
      }));

      const listPayload = {
        messaging_product: 'whatsapp',
        to: toNorm,
        type: 'interactive',
        interactive: {
          type: 'list',
          body: {
            text: 'Wybierz termin zajęć, który chcesz wykorzystać na odrabianie.'
          },
          action: {
            button: 'Wybierz termin',
            sections: [
              {
                title: 'Dostępne miejsca',
                rows: rowsList
              }
            ]
          }
        }
      };

      const listRes = await postWA({ phoneId: WA_PHONE_ID, payload: listPayload });
      const listWaMessageId = listRes.data?.messages?.[0]?.id || null;
      const listStatus = listRes.ok ? 'sent' : 'error';
      const listReason = listRes.ok
        ? null
        : (listRes.status ? `http_${listRes.status}` : 'send_failed');

      await auditOutbound({
        userId: u.userId,
        to: toNorm,
        body: 'WEEKLY_SLOTS_LIST: ' + rowsList.map(r => r.title).join(' | '),
        messageType: 'interactive_list',
        status: listStatus,
        reason: listReason,
        waMessageId: listWaMessageId
      });

      const offerIds = listOffers.map(o => o.offer_id);
      const newStatus = listRes.ok ? 'sent' : 'error';

      await client.query(
        `
        UPDATE public.slot_offers
           SET status = $2,
               updated_at = now()
         WHERE id = ANY($1::bigint[])
        `,
        [offerIds, newStatus]
      );
    }

    console.log('[CRON] weekly_slots_broadcast done, users:', byUser.size);
  } catch (err) {
    console.error('[CRON] weekly_slots_broadcast error', err);
  } finally {
    client.release();
  }
}

async function handleAbsenceInteractive({ client, m, sender }) {
  if (!sender || sender.type !== 'user' || !sender.active) return false;

  // wybór z listy zajęć
  if (m.type === 'interactive' && m.interactive?.type === 'list_reply') {
    const reply = m.interactive.list_reply;
    const id = reply?.id || '';

    if (!id.startsWith('absence_')) return false;

    if (id === 'absence_other_date') {
      await sendText({
        to: m.from,
        body: 'Napisz proszę wiadomość w formacie: "Zwalniam dd/mm" dla innego terminu.',
        userId: sender.id
      });
      await sendAbsenceMoreQuestion({ to: m.from, userId: sender.id });
      return true;
    }

    const parts = id.split('_'); // absence_YYYY-MM-DD_classTemplateId
    if (parts.length < 3) return false;

    const ymd = parts[1];
    const classTemplateId = Number(parts[2]) || null;

    // czy już jest absencja na ten dzień?
    const existing = await client.query(
      `
      SELECT id
      FROM public.absences
      WHERE user_id = $1
        AND class_template_id = $2
        AND session_date = $3::date
      LIMIT 1
      `,
      [sender.id, classTemplateId, ymd]
    );

    if (existing.rowCount > 0) {
      await sendText({
        to: m.from,
        body: `Na te zajęcia (${ymd}) jest już zgłoszona nieobecność.`,
        userId: sender.id
      });
      await sendAbsenceMoreQuestion({ to: m.from, userId: sender.id });
      return true;
    }

    const result = await processAbsence(client, sender.id, ymd, classTemplateId);

    if (result.ok) {
      await sendText({
        to: m.from,
        body: `✔️ Nieobecność ${ymd} została zgłoszona, miejsce zwolnione.`,
        userId: sender.id
      });
      await sendAbsenceMoreQuestion({ to: m.from, userId: sender.id });
      return true;
    }
    
    if (result.reason === 'already_absent') {
      await sendText({
        to: m.from,
        body: `Na te zajęcia (${ymd}) jest już zgłoszona nieobecność.`,
        userId: sender.id
      });
      await sendAbsenceMoreQuestion({ to: m.from, userId: sender.id });
      return true;
    }

    if (result.reason === 'past_date') {
      await sendText({
        to: m.from,
        body: 'Nie możesz zwolnić zajęć z datą w przeszłości.',
        userId: sender.id
      });
      await sendAbsenceMoreQuestion({ to: m.from, userId: sender.id });
      return true;
    }

    if (result.reason === 'no_enrollment_for_weekday') {
      await sendText({
        to: m.from,
        body: 'Nie udało się znaleźć Twoich zajęć w tym dniu. Sprawdź proszę termin lub wybierz inny z listy.',
        userId: sender.id
      });
      await sendAbsenceMoreQuestion({ to: m.from, userId: sender.id });
      return true;
    }

    await sendText({
      to: m.from,
      body: 'Coś poszło nie tak przy zgłaszaniu nieobecności. Spróbuj ponownie lub skontaktuj się ze studiem.',
      userId: sender.id
    });
    return true;
  }

  // obsługa przycisków Tak/Nie
  if (m.type === 'interactive' && m.interactive?.type === 'button_reply') {
    const replyId = m.interactive.button_reply?.id || '';

    if (replyId === 'absence_more_yes') {
      await sendUpcomingClassesMenu({ client, to: m.from, userId: sender.id });
      return true;
    }

    if (replyId === 'absence_more_no') {
      await sendText({
        to: m.from,
        body: 'Dziękujemy, nieobecności zostały zapisane.',
        userId: sender.id
      });
      return true;
    }
  }

  return false;
}

async function handleMakeupInteractive({ client, m, sender }) {
  if (!sender || sender.type !== 'user' || !sender.active) return false;
  if (m.type !== 'interactive' || m.interactive?.type !== 'list_reply') return false;

  const reply = m.interactive.list_reply;
  const id = reply?.id || '';
  if (!id.startsWith('makeup_')) return false;

  const offerId = parseInt(id.replace('makeup_', ''), 10);
  if (!offerId || Number.isNaN(offerId)) {
    await sendText({
      to: m.from,
      userId: sender.id,
      body: 'Nie udało się rozpoznać wybranego terminu. Spróbuj proszę ponownie.'
    });
    return true;
  }

  const userId = sender.id;
  const to = m.from;

  try {
    await client.query('BEGIN');

    // 1) Pobierz ofertę + slot + kredyt z blokadą
    const { rows, rowCount } = await client.query(
      `
      SELECT
        so.id          AS offer_id,
        so.user_id,
        so.slot_id,
        s.session_date,
        ct.start_time,
        g.name         AS group_name,
        uac.balance
      FROM public.slot_offers so
      JOIN public.slots s
        ON s.id = so.slot_id
      JOIN public.class_templates ct
        ON ct.id = s.class_template_id
      JOIN public.groups g
        ON g.id = ct.group_id
      JOIN public.user_absence_credits uac
        ON uac.user_id = so.user_id
      WHERE so.id = $1
        AND so.user_id = $2
        AND so.status IN ('pending', 'sent')
      FOR UPDATE
      `,
      [offerId, userId]
    );

    if (rowCount === 0) {
      await client.query('ROLLBACK');
      await sendText({
        to,
        userId,
        body: 'Nie udało się zarezerwować tego terminu. Wybierz proszę inny z listy.'
      });
      return true;
    }

    const offer = rows[0];

    if (offer.balance <= 0) {
      await client.query(
        `UPDATE public.slot_offers
           SET status = 'error', updated_at = now()
         WHERE id = $1`,
        [offerId]
      );
      await client.query('COMMIT');
      await sendText({
        to,
        userId,
        body: 'Nie masz już dostępnych nieobecności do odrabiania.'
      });
      return true;
    }

    // 2) Zajmij slot (race-safe)
    const updSlot = await client.query(
      `
      UPDATE public.slots
         SET status = 'taken',
             taken_by_user_id = $1,
             taken_at = now()
       WHERE id = $2
         AND status = 'open'
         AND taken_by_user_id IS NULL
       RETURNING id
      `,
      [userId, offer.slot_id]
    );

    if (updSlot.rowCount === 0) {
      await client.query(
        `UPDATE public.slot_offers
           SET status = 'expired', updated_at = now()
         WHERE id = $1`,
        [offerId]
      );
      await client.query('COMMIT');
      await sendText({
        to,
        userId,
        body: 'Ten termin został właśnie zajęty przez inną osobę. Wybierz proszę inny.'
      });
      return true;
    }

    // 3) Zmniejsz kredyt
    const updCred = await client.query(
      `
      UPDATE public.user_absence_credits
         SET balance = balance - 1,
             updated_at = now()
       WHERE user_id = $1
         AND balance > 0
       RETURNING balance
      `,
      [userId]
    );

    if (updCred.rowCount === 0) {
      // rollback slotu jeśli coś się omsknęło
      await client.query(
        `
        UPDATE public.slots
           SET status = 'open',
               taken_by_user_id = NULL,
               taken_at = NULL
         WHERE id = $1
        `,
        [offer.slot_id]
      );
      await client.query(
        `UPDATE public.slot_offers
           SET status = 'error', updated_at = now()
         WHERE id = $1`,
        [offerId]
      );
      await client.query('COMMIT');
      await sendText({
        to,
        userId,
        body: 'Nie udało się pobrać kredytu. Rezerwacja została anulowana.'
      });
      return true;
    }

    // 4) Oznacz ofertę jako zaakceptowaną
    await client.query(
      `
      UPDATE public.slot_offers
         SET status = 'accepted',
             updated_at = now()
       WHERE id = $1
      `,
      [offerId]
    );

    await client.query('COMMIT');

    // 5) Potwierdzenie
    const d = offer.session_date instanceof Date
      ? offer.session_date.toISOString().slice(0, 10)
      : String(offer.session_date).slice(0, 10);
    const [y, m, dd] = d.split('-');
    const time = (offer.start_time || '').toString().slice(0, 5);

    await sendText({
      to,
      userId,
      body: `✔️ Termin ${dd}/${m} ${time} ${offer.group_name} został zarezerwowany jako odrabianie.`
    });

    return true;
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (e) {}
    console.error('[makeup] error', err);
    await sendText({
      to,
      userId,
      body: 'Wystąpił błąd przy rezerwacji tego terminu. Spróbuj ponownie lub wybierz inny.'
    });
    return true;
  }
}

async function handleMainMenuInteractive({ client, m, sender }) {
  if (!sender || sender.type !== 'user' || !sender.active) return false;
  if (m.type !== 'interactive') return false;

  const itype = m.interactive?.type;

  // -------------------------
  // 1) Lista (MENU GŁÓWNE)
  // -------------------------
  if (itype === 'list_reply') {
    const id = m.interactive.list_reply?.id || '';

    if (id === 'menu_absence') {
      await sendUpcomingClassesMenu({ client, to: m.from, userId: sender.id });
      return true;
    }

    if (id === 'menu_makeup') {
      await sendMakeupMenu({ client, to: m.from, userId: sender.id });
      return true;
    }

    if (id === 'menu_credits') {
      const { rows } = await client.query(
        'SELECT balance FROM public.user_absence_credits WHERE user_id = $1',
        [sender.id]
      );
      const bal = rows[0]?.balance || 0;

      await sendText({
        to: m.from,
        body: `Masz ${bal} nieobecności do odrobienia.`,
        userId: sender.id
      });

      // follow-up: zapytanie, czy wrócić do menu czy zakończyć
      if (WA_TOKEN && WA_PHONE_ID) {
        const toNorm = normalizeTo(m.from);
        const payload = {
          messaging_product: 'whatsapp',
          to: toNorm,
          type: 'interactive',
          interactive: {
            type: 'button',
            body: {
              text: 'Czy chcesz wrócić do menu głównego czy zakończyć rozmowę?'
            },
            action: {
              buttons: [
                {
                  type: 'reply',
                  reply: { id: 'credits_menu', title: '⬅️ Menu główne' }
                },
                {
                  type: 'reply',
                  reply: { id: 'credits_end', title: 'Zakończ rozmowę' }
                }
              ]
            }
          }
        };

        const res = await postWA({ phoneId: WA_PHONE_ID, payload });
        const bodyLog = 'CREDITS_FOLLOWUP: [Menu główne] [Zakończ rozmowę]';

        const waMessageId = res.data?.messages?.[0]?.id || null;
        const status = res.ok ? 'sent' : 'error';
        const reason = res.ok
          ? null
          : (res.status ? `http_${res.status}` : 'send_failed');

        await auditOutbound({
          userId: sender.id,
          to: toNorm,
          body: bodyLog,
          messageType: 'interactive_buttons',
          status,
          reason,
          waMessageId
        });
      }

      return true;
    }

    if (id === 'menu_end') {
      await sendText({
        to: m.from,
        body: 'Dziękujemy za kontakt. Do zobaczenia na zajęciach!',
        userId: sender.id
      });
      return true;
    }

    return false;
  }

  // -------------------------
  // 2) Przyciski po "Ilość nieobecności"
  // -------------------------
  if (itype === 'button_reply') {
    const replyId = m.interactive.button_reply?.id || '';

    if (replyId === 'credits_menu') {
      await sendMainMenu({ to: m.from, userId: sender.id });
      return true;
    }

    if (replyId === 'credits_end') {
      await sendText({
        to: m.from,
        body: 'Dziękujemy za kontakt. Do zobaczenia na zajęciach!',
        userId: sender.id
      });
      return true;
    }

    return false;
  }

  return false;
}

function parseAbsenceCommand(text) {
  if (!text) return null;

  const normalized = text.trim().toLowerCase();

  // obsługa:
  // "Zwalniam dd/mm"
  // "Zwalniam dd.mm"
  // "Zwalniam dd-mm"
  // opcjonalnie: "Zwalniam dd/mm o gg:mm"
  const m = normalized.match(
    /^zwalniam\s+(\d{1,2})[./-](\d{1,2})(?:\s+o\s+\d{1,2}[:.]\d{2})?$/
  );
  if (!m) return null;

  const day = parseInt(m[1], 10);
  const month = parseInt(m[2], 10);
  if (day < 1 || day > 31 || month < 1 || month > 12) return null;

  const now = new Date();
  const year = now.getFullYear();

  const dd = String(day).padStart(2, '0');
  const mm = String(month).padStart(2, '0');

  const ymd = `${year}-${mm}-${dd}`;
  return { ymd };
}

function parseMainMenuChoice(text) {
  if (!text) return null;
  const t = text.trim();
  if (t === '1') return 'absence';
  if (t === '2') return 'makeup';
  if (t === '3') return 'credits';
  if (t === '4') return 'end';
  return null;
}

// =========================
// Główna operacja: zgłoszenie nieobecności + slot + kredyt
// =========================
async function processAbsence(client, userId, ymd, classTemplateIdHint = null) {
  try {
    await client.query('BEGIN');

    // 0) blokada dat w przeszłości
    const pastCheck = await client.query(
      'SELECT $1::date < CURRENT_DATE AS is_past',
      [ymd]
    );
    if (pastCheck.rows[0]?.is_past) {
      await client.query('ROLLBACK');
      return { ok: false, reason: 'past_date' };
    }

    let classTemplateId = classTemplateIdHint;

    // 1) ustalenie class_template_id

    if (classTemplateId) {
      // weryfikacja: user jest zapisany na te zajęcia i dzień tygodnia się zgadza
      const chk = await client.query(
        `
        SELECT 1
        FROM public.enrollments e
        JOIN public.class_templates ct ON ct.id = e.class_template_id
        WHERE e.user_id = $1
          AND ct.id = $2
          AND ct.is_active = true
          AND ct.weekday_iso = EXTRACT(ISODOW FROM $3::date)::int
        LIMIT 1
        `,
        [userId, classTemplateId, ymd]
      );
      if (chk.rowCount === 0) {
        await client.query('ROLLBACK');
        return { ok: false, reason: 'no_enrollment_for_weekday' };
      }
    } else {
      // tryb z komendy tekstowej: bierzemy pierwsze zajęcia usera w ten dzień
      const ctRes = await client.query(
        `
        SELECT ct.id AS class_template_id
        FROM public.enrollments e
        JOIN public.class_templates ct ON ct.id = e.class_template_id
        WHERE e.user_id = $1
          AND ct.is_active = true
          AND ct.weekday_iso = EXTRACT(ISODOW FROM $2::date)::int
        ORDER BY ct.start_time
        LIMIT 1
        `,
        [userId, ymd]
      );
      if (ctRes.rowCount === 0) {
        await client.query('ROLLBACK');
        return { ok: false, reason: 'no_enrollment_for_weekday' };
      }
      classTemplateId = ctRes.rows[0].class_template_id;
    }

    // 2) czy absencja już istnieje
    const existingAbs = await client.query(
      `
      SELECT id
      FROM public.absences
      WHERE user_id = $1
        AND class_template_id = $2
        AND session_date = $3::date
      LIMIT 1
      `,
      [userId, classTemplateId, ymd]
    );

    if (existingAbs.rowCount > 0) {
      await client.query('ROLLBACK');
      return {
        ok: false,
        reason: 'already_absent',
        absence_id: existingAbs.rows[0].id,
        class_template_id: classTemplateId
      };
    }

    // 3) nowa absencja
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
      VALUES ($1, $2, $3::date, 'whatsapp', now(), now())
      RETURNING id
      `,
      [userId, classTemplateId, ymd]
    );
    const absenceId = absRes.rows[0].id;

    // 4) wolny slot (unikamy duplikatu na tym samym absence_id, jeśli masz constraint, ON CONFLICT zadziała)
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
      ON CONFLICT DO NOTHING
      `,
      [classTemplateId, ymd, absenceId]
    );

    // 5) aktualizacja kredytu
    await client.query(
      `
      INSERT INTO public.user_absence_credits (user_id, balance, updated_at)
      VALUES ($1, 1, now())
      ON CONFLICT (user_id)
      DO UPDATE SET
        balance = user_absence_credits.balance + 1,
        updated_at = now()
      `,
      [userId]
    );

    await client.query('COMMIT');
    return { ok: true, absence_id: absenceId, class_template_id: classTemplateId };
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[processAbsence] error', err);
    return { ok: false, reason: 'exception', error: err.message };
  }
}

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

          let handled = false;

          // 1) Najpierw obsługa odpowiedzi interaktywnych (menu + Tak/Nie)
          if (m.type === 'interactive') {
            handled = await handleMainMenuInteractive({ client, m, sender });
            if (!handled) {
              handled = await handleMakeupInteractive({ client, m, sender });
            }
            if (!handled) {
              handled = await handleAbsenceInteractive({ client, m, sender });
            }
          }

          // 2) Obsługa typów nadawcy (jeśli nie przejęła tego logika absencji)
          if (!handled) {
            if (sender.type === 'none') {
              await sendText({
                to: m.from,
                body: 'Ten numer nie jest przypisany do żadnego użytkownika. Jeśli chcesz dołączyć do zajęć, skontaktuj się ze studiem przez formularz kontaktowy na stronie https://agnieszkapilatesklasyczny.pl/'
              });
              handled = true;
            } else if (sender.type === 'instructor') {
              // brak akcji
              handled = true;
            } else if (sender.type === 'user' && !sender.active) {
              await sendText({
                to: m.from,
                body: 'Dziękujemy za wiadomość.',
                userId: sender.id
              });
              handled = true;
            }
          }

          // 3) Aktywny user: powitanie + obsługa "Zwalniam dd/mm"
          if (!handled && sender.type === 'user' && sender.active) {
            let localHandled = false;

              if (m.text?.body) {
                const choice = parseMainMenuChoice(m.text.body);
                
                if (choice === 'absence') {
                  await sendUpcomingClassesMenu({ client, to: m.from, userId: sender.id });
                  localHandled = true;
                } else if (choice === 'makeup') {
                  await sendText({
                    to: m.from,
                    body: 'Odrabianie zajęć: napisz proszę termin, który Cię interesuje, a studio potwierdzi dostępność.',
                    userId: sender.id
                  });
                  localHandled = true;

                } else if (choice === 'credits') {
                  const { rows } = await client.query(
                    'SELECT balance FROM public.user_absence_credits WHERE user_id = $1',
                    [sender.id]
                  );
                  const bal = rows[0]?.balance || 0;
                    await sendText({
                      to: m.from,
                      body: `Masz ${bal} nieobecności do odrobienia.`,
                      userId: sender.id
                    });

                    localHandled = true;
                  } else if (choice === 'end') {
                    await sendText({
                      to: m.from,
                      body: 'Dziękujemy za kontakt. Do zobaczenia!',
                      userId: sender.id
                    });
                    localHandled = true;
                  }
                  // jeśli to nie był wybór z menu, sprawdzamy "zwalniam dd/mm"
                  if (!localHandled) {
                    const parsed = parseAbsenceCommand(m.text.body);
                    if (parsed) {
                      const result = await processAbsence(client, sender.id, parsed.ymd);

                      if (result.ok) {
                        await sendText({
                          to: m.from,
                          body: `✔️ Nieobecność ${parsed.ymd} została zgłoszona, miejsce zwolnione.`,
                          userId: sender.id
                        });
                        await sendAbsenceMoreQuestion({ to: m.from, userId: sender.id });
                        localHandled = true;
                      } else if (result.reason === 'past_date') {
                        await sendText({
                          to: m.from,
                          body: 'Nie możesz zwolnić zajęć z datą w przeszłości.',
                          userId: sender.id
                        });
                        localHandled = true;
                      } else if (result.reason === 'already_absent') {
                        await sendText({
                          to: m.from,
                          body: 'Na te zajęcia jest już zgłoszona nieobecność.',
                          userId: sender.id
                        });
                        await sendAbsenceMoreQuestion({ to: m.from, userId: sender.id });
                        localHandled = true;
                      } else if (result.reason === 'no_enrollment_for_weekday') {
                        await sendText({
                          to: m.from,
                          body: 'Nie znalazłem Twoich zajęć w tym terminie.',
                          userId: sender.id
                        });
                        await sendUpcomingClassesMenu({
                          client,
                          to: m.from,
                          userId: sender.id
                        });
                        localHandled = true;
                      } else {
                        await sendText({
                          to: m.from,
                          body: 'Coś poszło nie tak przy zgłaszaniu nieobecności. Spróbuj ponownie lub skontaktuj się ze studiem.',
                          userId: sender.id
                        });
                        localHandled = true;
                      }
                    }
                  }
                  // jeśli dalej nic nie pasuje --> dopiero wtedy powitanie i menu
                  if (!localHandled) {
                    await sendText({
                      to: m.from,
                      body: `Cześć ${sender.name}!`,
                      userId: sender.id
                    });
                    await sendMainMenu({ to: m.from, userId: sender.id });
                    localHandled = true;
                  }
              }

              handled = handled || localHandled;
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
// CRON JOBS
// =========================

if (WA_TOKEN && WA_PHONE_ID) {
  cron.schedule('30 10 * * 0', () => runWeeklySlotsJob(), {
    timezone: 'Europe/Warsaw'
  });

  cron.schedule('0 18 * * 0', () => sendAbsenceReminderTemplate(), {
    timezone: 'Europe/Warsaw'
  });

  cron.schedule('0 20 * * 0', () => runWeeklySlotsBroadcast(), {
    timezone: 'Europe/Warsaw'
  });

  console.log('[CRON] Scheduled weekly_slots (Sun 10:30), absence_reminder (Sun 18:00), weekly_slots_broadcast (Sun 20:00)');
} else {
  console.log('[CRON] Skipping CRON scheduling (missing WA config)');
}

// Niedziela 20:00 – broadcast wolnych slotów do kwalifikujących się użytkowników
cron.schedule(
  '0 20 * * 0',
  async () => {
    await runWeeklySlotsBroadcast();
  },
  { timezone: 'Europe/Warsaw' }
);

app.get('/debug/run-weekly-slots', async (req, res) => {
  // prosty prymitywny safeguard
  const token = req.query.token;
  if (!token || token !== process.env.DEBUG_TOKEN) {
    return res.status(403).json({ ok: false, error: 'forbidden' });
  }

  try {
    await runWeeklySlotsBroadcast();
    return res.json({ ok: true });
  } catch (err) {
    console.error('[DEBUG] run-weekly-slots error', err);
    return res.status(500).json({ ok: false, error: 'internal_error' });
  }
});

// =========================
// HEALTH / START
// =========================
app.get('/health', (req, res) => res.status(200).send('ok'));
const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => console.log(`Webhook listening on :${PORT}`));