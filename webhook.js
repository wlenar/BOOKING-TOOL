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

async function sendInstructorMenu({ to, instructorId }) {
  const toNorm = normalizeTo(to);

  if (!WA_TOKEN || !WA_PHONE_ID) {
    await auditOutbound({
      userId: null,
      to: toNorm,
      body: 'INSTR_MENU: pominięte (brak konfiguracji WhatsApp API)',
      messageType: 'interactive_list',
      status: 'skipped',
      reason: 'missing_credentials'
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
        text: '📚 Panel instruktora\nWybierz, co chcesz sprawdzić:'
      },
      action: {
        button: '📋 Otwórz menu',
        sections: [
          {
            title: 'Twoje narzędzia',
            rows: [
              {
                id: 'instr_today',
                title: '📅 Zajęcia na dziś',
                description: 'Plan Twoich grup na dzisiaj'
              },
              {
                id: 'instr_tomorrow',
                title: '📆 Zajęcia na jutro',
                description: 'Plan Twoich grup na jutro'
              },
              {
                id: 'instr_absences_7d',
                title: '📝 Nieobecności 7 dni',
                description: 'Kto odwoływał zajęcia'
              },
              {
                id: 'instr_add_slot',
                title: '➕ Dodaj wolne miejsce',
                description: 'Otwórz dodatkowy slot'
              },
              {
                id: 'instr_stats_7d',
                title: '📊 Statystyki 7 dni',
                description: 'Frekwencja i odrabiania'
              },
              {
                id: 'instr_end',
                title: '🏁 Zakończ',
                description: 'Zakończ rozmowę'
              }
            ]
          }
        ]
      }
    }
  };

  const res = await postWA({ phoneId: WA_PHONE_ID, payload });

  const bodyLog = 'INSTR_MENU: [today] [tomorrow] [absences_7d] [add_slot] [stats_7d] [end]';
  const waMessageId = res.data?.messages?.[0]?.id || null;
  const status = res.ok ? 'sent' : 'error';
  const reason = res.ok ? null : (res.status ? `http_${res.status}` : 'send_failed');

  await auditOutbound({
    userId: null,
    to: toNorm,
    body: bodyLog,
    messageType: 'interactive_list',
    status,
    reason,
    waMessageId
  });
}
// =========================
// DYNAMICZNE EMOJI POWITANIA
// =========================

function getSeasonEmoji() {
  const today = new Date();
  const month = today.getMonth() + 1; // 1-12
  const day = today.getDate();

  // Święta Bożego Narodzenia
  if (month === 12 && day >= 24 && day <= 26) return "🎄";

  // Zima
  if (month === 12 || month === 1 || month === 2) return "❄️";

  // Wiosna
  if (month >= 3 && month <= 5) return "🌱";

  // Lato
  if (month >= 6 && month <= 8) return "🌞";

  // Wczesna jesień
  if (month === 9 || month === 10) return "🍂";

  // Późna jesień (listopad)
  if (month === 11) return "🍁";

  // Domyślnie
  return "🌟";
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
    LEFT JOIN public.banking_holidays bh
      ON bh.holiday_date = d::date
     AND bh.is_active = true
    WHERE EXTRACT(ISODOW FROM d) = ct.weekday_iso
      AND bh.holiday_date IS NULL        -- ⬅️ kluczowe: odfiltruj święta
    ORDER BY session_date, ct.start_time, class_template_id;
  `;
  const res = await client.query(sql, [userId]);
  return res.rows;
}

async function sendMainMenu({ to, userId }) {
  const toNorm = normalizeTo(to);

  if (!WA_TOKEN || !WA_PHONE_ID) {
    await auditOutbound({
      userId,
      to: toNorm,
      body: 'MENU_GLOWNE: pominięte (brak konfiguracji WhatsApp API)',
      messageType: 'interactive_list',
      status: 'skipped',
      reason: 'missing_credentials'
    });
    return;
  }

  const emoji = getSeasonEmoji();

  const payload = {
    messaging_product: 'whatsapp',
    to: toNorm,
    type: 'interactive',
    interactive: {
      type: 'list',
      body: {
        text: '${getSeasonEmoji()} Witaj ponownie!\n\nWybierz, co chcesz zrobić 👇'
      },
      action: {
        button: '📋 Otwórz menu',
        sections: [
          {
            title: 'Dostępne opcje',
            rows: [
              {
                id: 'menu_absence',
                title: '📅 Zgłoś nieobecność',
                description: 'Zwolnij miejsce na zajęcia'
              },
              {
                id: 'menu_makeup',
                title: '🎯 Odrób zajęcia',
                description: 'Zarezerwuj wolny termin'
              },
              {
                id: 'menu_credits',
                title: '🔢 Moje nieobecności',
                description: 'Sprawdź ile masz do odrobienia'
              },
              {
                id: 'menu_end',
                title: '🏁 Zakończ rozmowę',
                description: 'Zamknij czat bez zmian'
              }
            ]
          }
        ]
      }
    }
  };

  const res = await postWA({ phoneId: WA_PHONE_ID, payload });

  const bodyLog =
    'MENU_GLOWNE: [Zgłoś nieobecność] [Odrób zajęcia] [Moje nieobecności] [Zakończ rozmowę]';

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
}

async function sendCreditsInfoAndFollowup({ client, to, userId }) {
  const { rows } = await client.query(
    'SELECT balance FROM public.user_absence_credits WHERE user_id = $1',
    [userId]
  );
  const bal = rows[0]?.balance || 0;

  const toNorm = normalizeTo(to);

  // podstawowa informacja o liczbie nieobecności
  await sendText({
    to: toNorm,
    userId,
    body: `Masz ${bal} nieobecności do odrobienia.`
  });

  // jeśli brak WA config – kończymy na samym tekście
  if (!WA_TOKEN || !WA_PHONE_ID) return;

  // brak kredytów → przyciski "Menu główne / Zakończ rozmowę"
  if (bal <= 0) {
    const payload = {
      messaging_product: 'whatsapp',
      to: toNorm,
      type: 'interactive',
      interactive: {
        type: 'button',
        body: {
          text: 'Co chcesz zrobić dalej?'
        },
        action: {
          buttons: [
            {
              type: 'reply',
              reply: { id: 'no_credits_menu', title: '🏠 Menu główne' }
            },
            {
              type: 'reply',
              reply: { id: 'no_credits_end', title: '🏁 Zakończ rozmowę' }
            }
          ]
        }
      }
    };

    const res = await postWA({ phoneId: WA_PHONE_ID, payload });
    const bodyLog = 'NO_CREDITS_FOLLOWUP: [Menu główne] [Zakończ rozmowę]';
    const waMessageId = res.data?.messages?.[0]?.id || null;
    const status = res.ok ? 'sent' : 'error';
    const reason = res.ok
      ? null
      : (res.status ? `http_${res.status}` : 'send_failed');

    await auditOutbound({
      userId,
      to: toNorm,
      body: bodyLog,
      messageType: 'interactive_buttons',
      status,
      reason,
      waMessageId
    });
    return;
  }

  // są kredyty → przyciski "Wolne terminy / Menu"
  const payload = {
    messaging_product: 'whatsapp',
    to: toNorm,
    type: 'interactive',
    interactive: {
      type: 'button',
      body: {
        text: 'Co chcesz zrobić dalej?'
      },
      action: {
        buttons: [
          {
            type: 'reply',
            reply: { id: 'credits_makeup', title: '🎯 Wolne terminy' }
          },
          {
            type: 'reply',
            reply: { id: 'credits_menu', title: '🏠 Menu' }
          }
        ]
      }
    }
  };

  const res = await postWA({ phoneId: WA_PHONE_ID, payload });
  const bodyLog = 'CREDITS_FOLLOWUP: [Wolne terminy] [Menu]';
  const waMessageId = res.data?.messages?.[0]?.id || null;
  const status = res.ok ? 'sent' : 'error';
  const reason = res.ok
    ? null
    : (res.status ? `http_${res.status}` : 'send_failed');

  await auditOutbound({
    userId,
    to: toNorm,
    body: bodyLog,
    messageType: 'interactive_buttons',
    status,
    reason,
    waMessageId
  });
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
    .slice(0, 10)
    .map((row) => {
      const rawDate = row.session_date;
      const iso = rawDate instanceof Date
        ? rawDate.toISOString().slice(0, 10)
        : String(rawDate).slice(0, 10); // YYYY-MM-DD

      const [y, m, d] = iso.split('-');
      const yy = y.slice(2, 4);

      let title = `${d}/${m}/${yy} ${row.group_name}`;
      if (title.length > 24) {
        title = title.slice(0, 24);
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
        text: '📅 Wybierz zajęcia, które chcesz zwolnić:'
      },
      action: {
        button: '🗓️ Wybierz termin',
        sections: [
          {
            title: 'Twoje najbliższe zajęcia',
            rows: sectionRows
          },
          {
            title: 'Inne opcje',
            rows: [
              {
                id: 'absence_other_date',
                title: '📆 Inny termin',
                description: 'Podam datę w wiadomości'
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
            reply: { id: 'absence_more_yes', title: '➕ Tak, kolejną' }
          },
          {
            type: 'reply',
            reply: { id: 'absence_more_no', title: '🏠 Menu główne' }
          }
        ]
      }
    }
  };

  const res = await postWA({ phoneId: WA_PHONE_ID, payload });

  const bodyLog = 'ABSENCE_MORE: [Tak, kolejną] [Menu główne]';

  const waMessageId = res.data?.messages?.[0]?.id || null;
  const status = res.ok ? 'sent' : 'error';
  const reason = res.ok
    ? null
    : (res.status ? `http_${res.status}` : 'send_failed');

  await auditOutbound({
    userId,
    to: toNorm,
    body: bodyLog,
    messageType: 'interactive_buttons',
    status,
    reason,
    waMessageId
  });

  return res;
}

async function sendMakeupMenu({ client, to, userId }) {
  const toNorm = normalizeTo(to);

  const userRes = await client.query(
    `
    SELECT
      u.id AS user_id,
      u.is_active,
      u.level_id,
      COALESCE(c.balance, 0) AS credits,
      COALESCE(uhp.max_home_price, 999999::numeric) AS max_home_price
    FROM public.users u
    LEFT JOIN public.user_absence_credits c
      ON c.user_id = u.id
    LEFT JOIN public.v_user_home_price uhp
      ON uhp.user_id = u.id
    WHERE u.id = $1
    `,
    [userId]
  );

  const u = userRes.rows[0];

  if (!u || !u.is_active) {
    return sendText({
      to: toNorm,
      userId,
      body: 'Twój numer nie jest aktywny w systemie. Skontaktuj się proszę ze studiem.'
    });
  }

  if ((u.credits || 0) <= 0) {
    // komunikat o braku nieobecności
    await sendText({
      to: toNorm,
      userId,
      body: 'Nie masz obecnie nieobecności do odrobienia.'
    });

    // follow-up: Menu główne / Zakończ rozmowę
    if (WA_TOKEN && WA_PHONE_ID) {
      const payload = {
        messaging_product: 'whatsapp',
        to: toNorm,
        type: 'interactive',
        interactive: {
          type: 'button',
          body: {
            text: 'Co chcesz zrobić dalej?'
          },
          action: {
            buttons: [
              {
                type: 'reply',
                reply: { id: 'no_credits_menu', title: '🏠 Menu główne' }
              },
              {
                type: 'reply',
                reply: { id: 'no_credits_end', title: '🏁 Zakończ rozmowę' }
              }
            ]
          }
        }
      };

      const res = await postWA({ phoneId: WA_PHONE_ID, payload });
      const bodyLog = 'NO_CREDITS_FOLLOWUP: [Menu główne] [Zakończ rozmowę]';
      const waMessageId = res.data?.messages?.[0]?.id || null;
      const status = res.ok ? 'sent' : 'error';
      const reason = res.ok
        ? null
        : (res.status ? `http_${res.status}` : 'send_failed');

      await auditOutbound({
        userId,
        to: toNorm,
        body: bodyLog,
        messageType: 'interactive_buttons',
        status,
        reason,
        waMessageId
      });
    }

    return;
  }

  const { rows } = await client.query(
    `
    WITH candidate_slots AS (
      SELECT
        os.session_date,
        os.session_date_ymd,
        os.session_time,
        os.class_template_id,
        os.group_name,
        COUNT(*) AS open_slots
      FROM public.v_open_slots_desc os
      LEFT JOIN public.enrollments e
        ON e.user_id = $1
       AND e.class_template_id = os.class_template_id
      WHERE
            os.session_date >= current_date
        AND os.session_date <  current_date + interval '7 days'
        AND os.free_capacity_remaining > 0
        AND (os.required_level IS NULL OR os.required_level <= $2)
        AND (os.price_per_session IS NULL OR os.price_per_session <= $3)
        AND e.user_id IS NULL
      GROUP BY
        os.session_date,
        os.session_date_ymd,
        os.session_time,
        os.class_template_id,
        os.group_name
      HAVING COUNT(*) > 0
    )
    SELECT *
    FROM candidate_slots
    ORDER BY session_date, session_time, group_name
    LIMIT 10
    `,
    [userId, u.level_id, u.max_home_price]
  );

  if (!rows || rows.length === 0) {
    return sendText({
      to: toNorm,
      userId,
      body: 'Aktualnie nie ma dostępnych wolnych miejsc do odrabiania w najbliższym tygodniu.'
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
    const iso = String(r.session_date_ymd || '').slice(0, 10);
    const [y, m, d] = iso.split('-');
    const dateLabel = `${d}/${m}`;
    const timeLabel = (r.session_time || '').toString().slice(0, 5);
    const count = Number(r.open_slots) || 1;
    const countLabel = count > 1 ? ` (${count} miejsca)` : '';

    let title = `${dateLabel} ${timeLabel}${countLabel}`;
    if (title.length > 24) title = title.slice(0, 24);

    const desc = r.group_name || '';

    return {
      id: `makeup_${iso}_${r.class_template_id}`,
      title,
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
        text: '✨ Wybierz termin, który chcesz zarezerwować:'
      },
      action: {
        button: '🎯 Zarezerwuj',
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

  const bodyLog = 'MAKEUP_MENU: ' + listRows.map(r => r.title).join(' | ');
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
        os.class_template_id,
        to_char(os.session_date, 'YYYY-MM-DD') AS session_date_ymd,
        to_char(os.session_date, 'DD.MM')     AS session_date_label,
        to_char(os.session_time, 'HH24:MI')   AS session_time_label,
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
        id: `makeup_${o.session_date_ymd}_${o.class_template_id}`,
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
        body: `✅ Zgłoszono Twoją nieobecność ${ymd}. Miejsce zostało zwolnione.`,
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
      await sendMainMenu({ to: m.from, userId: sender.id });
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

  // format: makeup_YYYY-MM-DD_classTemplateId
  const parts = id.split('_');
  if (parts.length !== 3) {
    await sendText({
      to: m.from,
      userId: sender.id,
      body: 'Nie udało się rozpoznać wybranego terminu. Spróbuj proszę ponownie.'
    });
    return true;
  }

  const sessionYmd = parts[1];
  const classTemplateId = parseInt(parts[2], 10);
  if (!classTemplateId || Number.isNaN(classTemplateId)) {
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

    // 1) kredyt z blokadą
    const creditRes = await client.query(
      `
      SELECT COALESCE(balance, 0) AS balance
      FROM public.user_absence_credits
      WHERE user_id = $1
      FOR UPDATE
      `,
      [userId]
    );
    const balance = creditRes.rows[0]?.balance || 0;
    if (balance <= 0) {
      await client.query('ROLLBACK');
      await sendText({
        to,
        userId,
        body: 'Nie masz już dostępnych nieobecności do odrabiania.'
      });
      return true;
    }

    // 2) wybierz 1 konkretny slot w tym terminie (zgodny z logiką menu)
    const slotRes = await client.query(
      `
      WITH u AS (
        SELECT
          u.id AS user_id,
          u.level_id,
          COALESCE(uhp.max_home_price, 999999::numeric) AS max_home_price
        FROM public.users u
        LEFT JOIN public.v_user_home_price uhp
          ON uhp.user_id = u.id
        WHERE u.id = $1
      )
      SELECT
        s.id            AS slot_id,
        os.session_date,
        os.session_time,
        os.group_name
      FROM public.slots s
      JOIN public.v_open_slots_desc os
        ON os.slot_id = s.id
      CROSS JOIN u
      LEFT JOIN public.enrollments e
        ON e.user_id = u.user_id
       AND e.class_template_id = os.class_template_id
      WHERE
          s.status = 'open'
        AND os.session_date = $2::date
        AND os.class_template_id = $3
        AND os.free_capacity_remaining > 0
        AND (os.required_level IS NULL OR os.required_level <= u.level_id)
        AND (os.price_per_session IS NULL OR os.price_per_session <= u.max_home_price)
        AND e.user_id IS NULL
        ORDER BY s.id
        LIMIT 1
        FOR UPDATE OF s SKIP LOCKED
      `,
      [userId, sessionYmd, classTemplateId]
    );

    if (slotRes.rowCount === 0) {
      await client.query('ROLLBACK');
      await sendText({
        to,
        userId,
        body: 'Wybrany termin nie jest już dostępny. Wybierz proszę inny termin.'
      });
      // wysylamy menu aktywnych terminów
      await sendMakeupMenu({ client, to, userId });
      
      return true;
    }

    const slot = slotRes.rows[0];

    // 3) oznacz slot jako zajęty przez usera
    await client.query(
      `
      UPDATE public.slots
      SET status = 'taken',
          taken_by_user_id = $1,
          taken_at = now(),
          updated_at = now()
      WHERE id = $2
      `,
      [userId, slot.slot_id]
    );

    // 4) zmniejsz kredyt
    await client.query(
      `
      UPDATE public.user_absence_credits
      SET balance = balance - 1,
          updated_at = now()
      WHERE user_id = $1
      `,
      [userId]
    );

    await client.query('COMMIT');

    const d = slot.session_date instanceof Date
      ? slot.session_date.toISOString().slice(0, 10)
      : String(slot.session_date).slice(0, 10);
    const [y, m2, d2] = d.split('-');
    const dateLabel = `${d2}/${m2}`;
    const timeLabel = (slot.session_time || '').toString().slice(0, 5);

    await sendText({
      to,
      userId,
      body: `✔️ Potwierdzam rezerwację miejsca do odrabiania: ${dateLabel} ${timeLabel}, ${slot.group_name}.`
    });

    return true;
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[handleMakeupInteractive] error', err);
    await sendText({
      to,
      userId,
      body: 'Coś poszło nie tak przy rezerwacji miejsca do odrabiania. Spróbuj ponownie lub skontaktuj się ze studiem.'
    });
    return true;
  }
}

async function handleMainMenuInteractive({ client, m, sender }) {
  if (!sender || sender.type !== 'user' || !sender.active) return false;
  if (m.type !== 'interactive') return false;

  const itype = m.interactive?.type;

  // 1) Odpowiedzi z listy (menu główne)
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
      await sendCreditsInfoAndFollowup({
        client,
        to: m.from,
        userId: sender.id
      });
    return true;
  }
    if (id === 'menu_end') {
      await sendText({
        to: m.from,
        body: '💛 Dziękujemy! Do zobaczenia w studiu!',
        userId: sender.id
      });
      return true;
    }

    return false;
  }

  // 2) Przyciski po "Moje nieobecności"
  if (itype === 'button_reply') {
    const replyId = m.interactive.button_reply?.id || '';

    if (replyId === 'credits_makeup') {
      await sendMakeupMenu({ client, to: m.from, userId: sender.id });
      return true;
    }

    if (replyId === 'credits_menu') {
      await sendMainMenu({ to: m.from, userId: sender.id });
      return true;
    }

    if (replyId === 'no_credits_menu') {
      await sendMainMenu({ to: m.from, userId: sender.id });
      return true;
    }

    if (replyId === 'no_credits_end') {
      await sendText({
        to: m.from,
        body: 'Dziękujemy za kontakt. Do zobaczenia!',
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
  let m = normalized.match(
    /^zwalniam\s+(\d{1,2})[./-](\d{1,2})(?:\s+o\s+\d{1,2}[:.]\d{2})?$/
  );

  if (!m) {
    m = normalized.match(
      /^(\d{1,2})[./-](\d{1,2})(?:\s+o\s+\d{1,2}[:.]\d{2})?$/
    );
  }
  if (!m) return null;

  const day = parseInt(m[1], 10);
  const month = parseInt(m[2], 10);
  if (day < 1 || day > 31 || month < 1 || month > 12) return null;

  const now = new Date();
  const thisYear = now.getFullYear();

  const dd = String(day).padStart(2, '0');
  const mm = String(month).padStart(2, '0');

  const candidateThisYear = new Date(`${thisYear}-${mm}-${dd}T00:00:00`);
  let year = thisYear;

  // jeśli jesteśmy w grudniu, a wpisana data "cofa" nas >7 dni → zakładamy przyszły rok
  if (
    now.getMonth() === 11 && // grudzień (0-based)
    candidateThisYear.getTime() + 7 * 24 * 60 * 60 * 1000 < now.getTime()
  ) {
    year = thisYear + 1;
  }

  return { ymd: `${year}-${mm}-${dd}` };
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
      // tryb z komendy tekstowej: pierwsze zajęcia usera w ten dzień
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

    // 4) wolny slot
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

    // 6) powiadomienie instruktora (best-effort, poza transakcją)
    try {
      const { rows: infoRows } = await client.query(
        `
        SELECT
          u.first_name                         AS user_first_name,
          COALESCE(u.last_name, '')            AS user_last_name,
          g.name                               AS group_name,
          to_char($2::date, 'DD.MM')           AS session_date_label,
          ct.start_time,
          i.first_name                         AS instr_first_name,
          COALESCE(
            i.phone_e164,
            CASE
              WHEN i.phone_raw IS NOT NULL
              THEN '+' || REGEXP_REPLACE(i.phone_raw, '^\\+?', '')
              ELSE NULL
            END
          )                                    AS instr_phone
        FROM public.users u
        JOIN public.enrollments e
          ON e.user_id = u.id
        JOIN public.class_templates ct
          ON ct.id = e.class_template_id
         AND ct.id = $3
        JOIN public.groups g
          ON g.id = ct.group_id
        JOIN public.instructors i
          ON i.id = g.instructor_id
        WHERE u.id = $1
        LIMIT 1
        `,
        [userId, ymd, classTemplateId]
      );

      const info = infoRows[0];
      if (info && info.instr_phone) {
        // identyfikacja WA-ID instruktora tak jak w inbox_messages (bez "+")
        const instrWaId = normalizeTo(info.instr_phone);

        // ostatnia interakcja instruktora z naszym numerem
        const { rows: inboundRows } = await client.query(
          `
          SELECT sent_ts
          FROM inbox_messages
          WHERE source = 'whatsapp'
            AND message_direction = 'inbound'
            AND from_wa_id = $1
          ORDER BY sent_ts DESC
          LIMIT 1
          `,
          [instrWaId]
        );

        const lastTs = inboundRows[0]?.sent_ts
          ? new Date(inboundRows[0].sent_ts)
          : null;
        const now = new Date();

        const hasInteractionLast24h =
          lastTs && (now.getTime() - lastTs.getTime()) < 24 * 60 * 60 * 1000;

        const userFullName =
          (info.user_first_name || '') +
          (info.user_last_name ? ` ${info.user_last_name}` : '');
        const who = userFullName.trim() || 'Klient';
        const absLabel = info.session_date_label;

        // TREŚĆ spójna z szablonem WhatsApp:
        // "Cześć <imie>\n\n<imie1> zgłosił(a) nieobezność dnia <nieobecnosc>"
        const unifiedText =
          `Cześć ${info.instr_first_name || 'Instruktorze'}\n\n` +
          `${who} zgłosił(a) nieobezność dnia ${absLabel}`;

        if (hasInteractionLast24h) {
          // mieliśmy kontakt <24h -> zwykły tekst
          await sendText({
            to: info.instr_phone,
            body: unifiedText,
            userId: null
          });
        } else {
          // brak kontaktu <24h -> template (formularz)
          await sendInstructorAbsenceTemplate({ info });
        }
      }
    } catch (notifyErr) {
      console.error('[processAbsence] notify instructor error', notifyErr);
    }

    return { ok: true, absence_id: absenceId, class_template_id: classTemplateId };
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[processAbsence] error', err);
    return { ok: false, reason: 'exception', error: err.message };
  }
}

async function sendInstructorAbsenceTemplate({ info }) {
  const toNorm = normalizeTo(info.instr_phone);

  // ⬇ PODMIEŃ nazwę szablonu na dokładną z Business Managera
  const TEMPLATE_NAME = 'update_do_trenera';

  if (!WA_TOKEN || !WA_PHONE_ID) {
    await auditOutbound({
      userId: null,
      to: toNorm,
      body: `TEMPLATE: ${TEMPLATE_NAME} (brak konfiguracji WhatsApp API)`,
      templateName: TEMPLATE_NAME,
      variables: null,
      messageType: 'template',
      status: 'skipped',
      reason: 'missing_config'
    });
    return { ok: false, reason: 'missing_config' };
  }

  const userFullName = (info.user_first_name || '') +
    (info.user_last_name ? ` ${info.user_last_name}` : '');
  const who = userFullName.trim() || 'Klient';

  const payload = {
    messaging_product: 'whatsapp',
    to: toNorm,
    type: 'template',
    template: {
      name: TEMPLATE_NAME,
      language: { code: 'pl' },
      components: [
        {
          type: 'body',
          parameters: [
            // {1} = imię instruktora
            { type: 'text', text: info.instr_first_name || 'Instruktorze' },
            // {2} = imię + nazwisko klienta
            { type: 'text', text: who },
            // {3} = data nieobecności, np. "12.11"
            { type: 'text', text: info.session_date_label }
          ]
        }
      ]
    }
  };

  const res = await postWA({ phoneId: WA_PHONE_ID, payload });

  const waMessageId = res.data?.messages?.[0]?.id || null;
  const status = res.ok ? 'sent' : 'error';
  const reason = res.ok
    ? null
    : (res.status ? `http_${res.status}` : 'send_failed');

  await auditOutbound({
    userId: null,
    to: toNorm,
    body: `TEMPLATE: ${TEMPLATE_NAME}`,
    templateName: TEMPLATE_NAME,
    variables: JSON.stringify({
      '{1}': info.instr_first_name || 'Instruktorze',
      '{2}': who,
      '{3}': info.session_date_label
    }),
    messageType: 'template',
    status,
    reason,
    waMessageId
  });

  return res;
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
            // najpierw panel instruktora (jeśli dotyczy)
            handled = await handleInstructorInteractive({ client, m, sender });

            if (!handled) {
              handled = await handleMainMenuInteractive({ client, m, sender });
            }
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
              await sendInstructorMenu({ to: m.from, instructorId: sender.id });
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
                  await sendMakeupMenu({ client, to: m.from, userId: sender.id });
                  localHandled = true;
                } else if (choice === 'credits') {
                  await sendCreditsInfoAndFollowup({
                    client,
                    to: m.from,
                    userId: sender.id
                  });  
                  localHandled = true;
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
                          body: `✅ Nieobecność ${parsed.ymd} została zgłoszona, miejsce zwolnione.`,
                          userId: sender.id
                        });
                        await sendAbsenceMoreQuestion({ to: m.from, userId: sender.id });
                        localHandled = true;
                      } else if (result.reason === 'past_date') {
                        await sendText({
                          to: m.from,
                          body: '❗️Nie możesz zwolnić zajęć z datą w przeszłości.',
                          userId: sender.id
                        });
                        localHandled = true;
                      } else if (result.reason === 'already_absent') {
                        await sendText({
                          to: m.from,
                          body: '💬Na te zajęcia jest już zgłoszona nieobecność.',
                          userId: sender.id
                        });
                        await sendAbsenceMoreQuestion({ to: m.from, userId: sender.id });
                        localHandled = true;
                      } else if (result.reason === 'no_enrollment_for_weekday') {
                        await sendText({
                          to: m.from,
                          body: '❗️Nie znalazłem Twoich zajęć w tym terminie.',
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
                          body: '❗️Coś poszło nie tak przy zgłaszaniu nieobecności. Spróbuj ponownie lub skontaktuj się ze studiem.',
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

async function sendAbsenceReminderTemplate() {
  if (!WA_TOKEN || !WA_PHONE_ID) {
    console.log('[CRON] absence_reminder skipped (missing WA config)');
    return;
  }

  const client = await pool.connect();

  try {
    console.log('[CRON] absence_reminder start');

    const { rows } = await client.query(
      `
      SELECT
        id AS user_id,
        first_name,
        COALESCE(
          phone_e164,
          CASE
            WHEN phone_raw IS NOT NULL
            THEN '+' || REGEXP_REPLACE(phone_raw, '^\\+?', '')
            ELSE NULL
          END
        ) AS phone
      FROM public.users
      WHERE is_active = true
        AND (
          phone_e164 IS NOT NULL
          OR phone_raw IS NOT NULL
        )
      `
    );

    if (!rows.length) {
      console.log('[CRON] absence_reminder no active users with phone');
      return;
    }

    for (const u of rows) {
      const toNorm = normalizeTo(u.phone);
      if (!toNorm) continue;

      const firstName = (u.first_name || '').trim() || 'Klientko/Kliencie';

      const payload = {
        messaging_product: 'whatsapp',
        to: toNorm,
        type: 'template',
        template: {
          name: 'absence_reminder',
          language: { code: 'pl' },
          components: [
            {
              type: 'body',
              parameters: [
                { type: 'text', text: firstName }
              ]
            }
          ]
        }
      };

      const res = await postWA({ phoneId: WA_PHONE_ID, payload });

      const waMessageId = res.data?.messages?.[0]?.id || null;
      const status = res.ok ? 'sent' : 'error';
      const reason = res.ok
        ? null
        : (res.status ? `http_${res.status}` : 'send_failed');

      await auditOutbound({
        userId: u.user_id,
        to: toNorm,
        body: 'TEMPLATE: absence_reminder',
        templateName: 'absence_reminder',
        variables: JSON.stringify({ '{1}': firstName }),
        messageType: 'template',
        status,
        reason,
        waMessageId
      });
    }

    console.log('[CRON] absence_reminder done, users:', rows.length);
  } catch (err) {
    console.error('[CRON] absence_reminder error', err);
  } finally {
    client.release();
  }
}

async function sendPaymentReminderTemplate() {
  if (!WA_TOKEN || !WA_PHONE_ID) {
    console.log('[CRON] payment_reminder skipped (missing WA config)');
    return;
  }

  const client = await pool.connect();

  try {
    console.log('[CRON] payment_reminder start');

    const { rows } = await client.query(
      `
      SELECT
        id AS user_id,
        COALESCE(
          phone_e164,
          CASE
            WHEN phone_raw IS NOT NULL
            THEN '+' || REGEXP_REPLACE(phone_raw, '^\\+?', '')
            ELSE NULL
          END
        ) AS phone
      FROM public.users
      WHERE is_active = true
        AND (
          phone_e164 IS NOT NULL
          OR phone_raw IS NOT NULL
        )
      `
    );

    if (!rows.length) {
      console.log('[CRON] payment_reminder no active users with phone');
      return;
    }

    for (const u of rows) {
      const toNorm = normalizeTo(u.phone);
      if (!toNorm) continue;

      const payload = {
        messaging_product: 'whatsapp',
        to: toNorm,
        type: 'template',
        template: {
          name: 'payment_reminder',
          language: { code: 'pl' }
          // brak components – template bez zmiennych
        }
      };

      const res = await postWA({ phoneId: WA_PHONE_ID, payload });
      
      if (!res.ok) {
        try {
          console.error(
            '[PAYMENT_REMINDER] WA error',
            res.status,
            JSON.stringify(res.data || {}, null, 2)
          );
        } catch (e) {
          console.error('[PAYMENT_REMINDER] WA error (no data)', res.status);
        }
      }

      const waMessageId = res.data?.messages?.[0]?.id || null;
      const status = res.ok ? 'sent' : 'error';
      const reason = res.ok
        ? null
        : (res.status ? `http_${res.status}` : 'send_failed');

      await auditOutbound({
        userId: u.user_id,
        to: toNorm,
        body: 'TEMPLATE: payment_reminder',
        templateName: 'payment_reminder',
        variables: null,
        messageType: 'template',
        status,
        reason,
        waMessageId
      });
    }

    console.log('[CRON] payment_reminder done, users:', rows.length);
  } catch (err) {
    console.error('[CRON] payment_reminder error', err);
  } finally {
    client.release();
  }
}

async function sendInstructorClassesForDay({ client, to, instructorId, dayOffset }) {
  const toNorm = normalizeTo(to);

  const { rows } = await client.query(
    `
    WITH target_day AS (
      SELECT (current_date + $2::int) AS d
    ),
    base AS (
      SELECT
        td.d                  AS session_date,
        ct.id                 AS class_template_id,
        ct.start_time,
        ct.end_time,
        g.name                AS group_name,
        g.max_capacity
      FROM target_day td
      JOIN public.class_templates ct
        ON ct.is_active = true
       AND EXTRACT(ISODOW FROM td.d) = ct.weekday_iso
      JOIN public.groups g
        ON g.id = ct.group_id
       AND g.is_active = true
       AND g.instructor_id = $1
    ),
    enr AS (
      SELECT
        e.class_template_id,
        COUNT(*) AS enrolled_count
      FROM public.enrollments e
      GROUP BY e.class_template_id
    ),
    os_open AS (
      SELECT
        s.class_template_id,
        s.session_date,
        COUNT(*) AS open_slots
      FROM public.slots s
      WHERE s.status = 'open'
      GROUP BY s.class_template_id, s.session_date
    )
    SELECT
      b.session_date,
      b.start_time,
      b.end_time,
      b.group_name,
      b.max_capacity,
      COALESCE(e.enrolled_count, 0) AS enrolled_count,
      COALESCE(o.open_slots, 0)     AS open_slots
    FROM base b
    LEFT JOIN enr e
      ON e.class_template_id = b.class_template_id
    LEFT JOIN os_open o
      ON o.class_template_id = b.class_template_id
     AND o.session_date = b.session_date
    ORDER BY b.start_time, b.group_name
    `,
    [instructorId, dayOffset]
  );

  if (!rows.length) {
    const label = dayOffset === 0 ? 'dzisiaj' : 'jutro';
    await sendText({
      to: toNorm,
      body: `Nie masz przypisanych zajęć na ${label}.`
    });
    return;
  }

  const label = dayOffset === 0 ? 'dzisiaj' : 'jutro';
  const lines = rows.map(r => {
    const timeFrom = r.start_time.toString().slice(0,5);
    const timeTo = r.end_time.toString().slice(0,5);
    return `• ${timeFrom}-${timeTo} ${r.group_name} (${r.enrolled_count}/${r.max_capacity}, wolne: ${r.open_slots})`;
  });

  await sendText({
    to: toNorm,
    body: `Twoje zajęcia na ${label}:\n` + lines.join('\n')
  });
}

async function sendInstructorAbsences7d({ client, to, instructorId }) {
  const toNorm = normalizeTo(to);

  const { rows } = await client.query(
    `
    SELECT
      a.session_date,
      ct.start_time,
      g.name        AS group_name,
      u.first_name,
      u.last_name
    FROM public.absences a
    JOIN public.class_templates ct
      ON ct.id = a.class_template_id
    JOIN public.groups g
      ON g.id = ct.group_id
     AND g.instructor_id = $1
    JOIN public.users u
      ON u.id = a.user_id
    WHERE a.session_date >= current_date
      AND a.session_date < current_date + interval '7 days'
    ORDER BY a.session_date ASC, ct.start_time, g.name, u.first_name, u.last_name
    `,
    [instructorId]
  );

  if (!rows.length) {
    await sendText({
      to: toNorm,
      body: 'W najbliższych 7 dniach nie masz zgłoszonych nieobecności w swoich grupach.'
    });
    return;
  }

  const lines = rows.map(r => {
    const d = r.session_date.toISOString().slice(0, 10);
    const [y, m, dd] = d.split('-');
    const time = r.start_time.toString().slice(0, 5);
    return `• ${dd}/${m} ${time} ${r.group_name}: ${r.first_name} ${r.last_name}`;
  });

  await sendText({
    to: toNorm,
    body: 'Nieobecności w Twoich grupach (najbliższe 7 dni):\n' + lines.join('\n')
  });
}

async function sendInstructorAddSlotMenu({ client, to, instructorId }) {
  const toNorm = normalizeTo(to);

  if (!WA_TOKEN || !WA_PHONE_ID) {
    await auditOutbound({
      userId: null,
      to: toNorm,
      body: 'INSTR_ADD_SLOT_MENU (brak konfiguracji WhatsApp API)',
      messageType: 'interactive_list',
      status: 'skipped',
      reason: 'missing_config'
    });
    return;
  }

  const { rows } = await client.query(
    `
    WITH days AS (
      SELECT generate_series(current_date, current_date + interval '6 days', interval '1 day')::date AS d
    )
    SELECT
      d.d                   AS session_date,
      ct.id                 AS class_template_id,
      ct.start_time,
      g.name                AS group_name
    FROM days d
    JOIN public.class_templates ct
      ON ct.is_active = true
     AND EXTRACT(ISODOW FROM d.d) = ct.weekday_iso
    JOIN public.groups g
      ON g.id = ct.group_id
     AND g.is_active = true
     AND g.instructor_id = $1
    WHERE
      -- tylko przyszłość:
      d.d > current_date
      OR (d.d = current_date AND ct.start_time > now()::time)
    ORDER BY d.d, ct.start_time, g.name
    LIMIT 10
    `,
    [instructorId]
  );

  if (!rows.length) {
    await sendText({
      to: toNorm,
      body: 'Brak dostępnych najbliższych terminów, dla których można dodać wolne miejsce.'
    });
    return;
  }

  const listRows = rows.map(r => {
    const iso = r.session_date.toISOString().slice(0,10); // YYYY-MM-DD
    const [y, m, d] = iso.split('-');
    const dateLabel = `${d}/${m}`;
    const timeLabel = r.start_time.toString().slice(0,5);
    let title = `${dateLabel} ${timeLabel}`;
    if (title.length > 24) title = title.slice(0,24);
    return {
      id: `instr_addslot_${iso}_${r.class_template_id}`,
      title,
      description: r.group_name.substring(0,80)
    };
  });

  const payload = {
    messaging_product: 'whatsapp',
    to: toNorm,
    type: 'interactive',
    interactive: {
      type: 'list',
      body: {
        text: 'Wybierz przyszłe zajęcia, dla których chcesz dodać jedno dodatkowe wolne miejsce:'
      },
      action: {
        button: '➕ Wybierz termin',
        sections: [
          {
            title: 'Najbliższe zajęcia',
            rows: listRows
          }
        ]
      }
    }
  };

  const res = await postWA({ phoneId: WA_PHONE_ID, payload });
  const bodyLog = 'INSTR_ADD_SLOT_MENU: ' + listRows.map(r => r.title).join(' | ');
  const waMessageId = res.data?.messages?.[0]?.id || null;
  const status = res.ok ? 'sent' : 'error';
  const reason = res.ok ? null : (res.status ? `http_${res.status}` : 'send_failed');

  await auditOutbound({
    userId: null,
    to: toNorm,
    body: bodyLog,
    messageType: 'interactive_list',
    status,
    reason,
    waMessageId
  });
}

async function handleInstructorAddSlotSelection({ client, m, sender }) {
  const toNorm = normalizeTo(m.from);

  const reply = m.interactive?.list_reply;
  const id = reply?.id || '';
  if (!id.startsWith('instr_addslot_')) return false;

  // format: instr_addslot_YYYY-MM-DD_classTemplateId
  const parts = id.split('_');
  if (parts.length !== 4) {
    await sendText({
      to: toNorm,
      body: 'Nie udało się rozpoznać terminu. Spróbuj ponownie.'
    });
    return true;
  }

  const ymd = parts[2];
  const classTemplateId = parseInt(parts[3], 10);
  if (!classTemplateId || Number.isNaN(classTemplateId)) {
    await sendText({
      to: toNorm,
      body: 'Nie udało się rozpoznać terminu. Spróbuj ponownie.'
    });
    return true;
  }

  // blokada dat wstecz
  const { rows: pastRows } = await client.query(
    'SELECT $1::date < current_date AS is_past',
    [ymd]
  );
  if (pastRows[0]?.is_past) {
    await sendText({
      to: toNorm,
      body: 'Nie możesz dodać miejsca na termin w przeszłości.'
    });
    return true;
  }

  // sprawdź, czy class_template należy do tego instruktora
  const { rows } = await client.query(
    `
    SELECT
      g.instructor_id,
      g.name        AS group_name,
      ct.start_time AS start_time
    FROM public.class_templates ct
    JOIN public.groups g ON g.id = ct.group_id
    WHERE ct.id = $1
    LIMIT 1
    `,
    [classTemplateId]
  );

  const row = rows[0];
  if (!row || row.instructor_id !== sender.id) {
    await sendText({
      to: toNorm,
      body: 'Nie możesz dodać miejsca do tych zajęć.'
    });
    return true;
  }

  // ręczne otwarcie dodatkowego miejsca (bez ograniczania do max_capacity)
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
    VALUES ($1, $2::date, NULL, 'open', NULL, NULL, now(), now())
    `,
    [classTemplateId, ymd]
  );

  const [Y, M, D] = ymd.split('-');
  const timeLabel = row.start_time
    ? row.start_time.toString().slice(0, 5)
    : '';
  await sendText({
    to: toNorm,
    body: `Dodano jedno wolne miejsce na ${D}/${M} ${timeLabel} (${row.group_name}).`
  });

  return true;
}

async function sendInstructorStats7d({ client, to, instructorId }) {
  const toNorm = normalizeTo(to);

  const { rows } = await client.query(
    `
    WITH days AS (
      SELECT generate_series(current_date - interval '6 days', current_date, interval '1 day')::date AS d
    ),
    classes AS (
      SELECT DISTINCT
        d.d               AS session_date,
        ct.id             AS class_template_id
      FROM days d
      JOIN public.class_templates ct
        ON ct.is_active = true
       AND EXTRACT(ISODOW FROM d.d) = ct.weekday_iso
      JOIN public.groups g
        ON g.id = ct.group_id
       AND g.is_active = true
       AND g.instructor_id = $1
    ),
    abs AS (
      SELECT COUNT(*) AS total_absences
      FROM public.absences a
      JOIN classes c
        ON c.class_template_id = a.class_template_id
       AND c.session_date = a.session_date
    ),
    open_slots AS (
      SELECT COUNT(*) AS total_open_slots
      FROM public.slots s
      JOIN classes c
        ON c.class_template_id = s.class_template_id
       AND c.session_date = s.session_date
      WHERE s.status = 'open'
    ),
    taken_slots AS (
      SELECT COUNT(*) AS total_makeups
      FROM public.slots s
      JOIN classes c
        ON c.class_template_id = s.class_template_id
       AND c.session_date = s.session_date
      WHERE s.status = 'taken'
    )
    SELECT
      (SELECT COUNT(*) FROM classes)                      AS total_classes,
      COALESCE((SELECT total_absences  FROM abs),0)       AS total_absences,
      COALESCE((SELECT total_open_slots FROM open_slots),0) AS total_open_slots,
      COALESCE((SELECT total_makeups   FROM taken_slots),0) AS total_makeups
    `,
    [instructorId]
  );

  const s = rows[0] || {
    total_classes: 0,
    total_absences: 0,
    total_open_slots: 0, // zostawiamy w obiekcie, ale nie używamy
    total_makeups: 0
  };

  const usedMakeups = s.total_makeups;
  const unusedMakeups = Math.max(s.total_absences - s.total_makeups, 0);

  await sendText({
    to: toNorm,
    body:
      'Ostatnie 7 dni (Twoje grupy):\n' +
      `• Zajęcia: ${s.total_classes}\n` +
      `• Zgłoszone nieobecności: ${s.total_absences}\n` +
      `• Odrabiane miejsca: ${usedMakeups}\n` +
      `• Niewykorzystane miejsca: ${unusedMakeups}`
  });
}

async function handleInstructorInteractive({ client, m, sender }) {
  if (!sender || sender.type !== 'instructor' || !sender.active) return false;
  if (m.type !== 'interactive') return false;

  const itype = m.interactive?.type || '';

  // wybory z listy głównej
  if (itype === 'list_reply') {
    const id = m.interactive.list_reply?.id || '';

    if (id === 'instr_today') {
      await sendInstructorClassesForDay({ client, to: m.from, instructorId: sender.id, dayOffset: 0 });
      return true;
    }

    if (id === 'instr_tomorrow') {
      await sendInstructorClassesForDay({ client, to: m.from, instructorId: sender.id, dayOffset: 1 });
      return true;
    }

    if (id === 'instr_absences_7d') {
      await sendInstructorAbsences7d({ client, to: m.from, instructorId: sender.id });
      return true;
    }

    if (id === 'instr_add_slot') {
      await sendInstructorAddSlotMenu({ client, to: m.from, instructorId: sender.id });
      return true;
    }

    if (id === 'instr_stats_7d') {
      await sendInstructorStats7d({ client, to: m.from, instructorId: sender.id });
      return true;
    }

    if (id === 'instr_end') {
      await sendText({
        to: m.from,
        body: 'Dziękujemy. Panel instruktora zamknięty.'
      });
      return true;
    }

    // wybór konkretnego terminu do dodania slota
    if (id.startsWith('instr_addslot_')) {
      return await handleInstructorAddSlotSelection({ client, m, sender });
    }

    return false;
  }

  // na wszelki wypadek obsłuż także list_reply z add_slot w tym samym miejscu
  if (itype === 'button_reply') {
    // na razie brak przycisków w menu instruktora
    return false;
  }

  return false;
}


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

  // nowy CRON: 25-go dnia miesiąca, 18:00
  cron.schedule('0 18 22 * *', () => sendPaymentReminderTemplate(), {
    timezone: 'Europe/Warsaw'
  });

  console.log('[CRON] Scheduled weekly_slots (Sun 10:30), absence_reminder (Sun 18:00), weekly_slots_broadcast (Sun 20:00), payment_reminder (25th 18:00)');
} else {
  console.log('[CRON] Skipping CRON scheduling (missing WA config)');
}

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

app.get('/debug/run-payment-reminder', async (req, res) => {
  const token = req.query.token;
  if (!token || token !== process.env.DEBUG_TOKEN) {
    return res.status(403).json({ ok: false, error: 'forbidden' });
  }

  try {
    await sendPaymentReminderTemplate();
    return res.json({ ok: true });
  } catch (err) {
    console.error('[DEBUG] run-payment-reminder error', err);
    return res.status(500).json({ ok: false, error: 'internal_error' });
  }
});

// =========================
// HEALTH / START
// =========================
app.get('/health', (req, res) => res.status(200).send('ok'));
const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => console.log(`Webhook listening on :${PORT}`));