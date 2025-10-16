// /api/sync-confirmations.js
require('dotenv').config();
const axios = require('axios');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const tz = require('dayjs/plugin/timezone');
const PQueue = require('p-queue').default;

dayjs.extend(utc);
dayjs.extend(tz);

const TIMEZONE = process.env.TZ || 'America/Sao_Paulo';
const DRY_RUN = String(process.env.DRY_RUN || 'false').toLowerCase() === 'true';
const CONCURRENCY = Number(process.env.CONCURRENCY || 4);
const INTERVAL_MS = Number(process.env.INTERVAL_MS || 1000);
const INTERVAL_CAP = Number(process.env.INTERVAL_CAP || 8);

// ===== Datasigh (SEM Bearer) =====
const DATASIGH_BASE_URL = process.env.DATASIGH_BASE_URL || 'https://ws.datasigh.com.br/api/integracao/v1';
const DATASIGH_API_KEY = process.env.DATASIGH_API_KEY || ''; // formato: integration_hash:client_hash
const DATASIGH_DATE_FORMAT = process.env.DATASIGH_DATE_FORMAT || 'DD/MM/YYYY';

// ===== TalkBI =====
const TALKBI_BASE_URL = process.env.TALKBI_BASE_URL || 'https://chat.talkbi.com.br/api';
const TALKBI_API_KEY = process.env.TALKBI_API_KEY || '';
const TALKBI_FLOW_NAME = process.env.TALKBI_FLOW_NAME || '';

const http = axios.create({ timeout: 15000 });

/* ============ Utils ============ */
function tomorrowStr() {
  return dayjs().tz(TIMEZONE).add(1, 'day').format('YYYY-MM-DD'); // interno
}
function dsFormat(dateLike) {
  return dayjs(dateLike).tz(TIMEZONE).format(DATASIGH_DATE_FORMAT); // Datasigh exige DD/MM/YYYY
}
function normalizePhoneBR(phone) {
  if (!phone) return null;
  let d = String(phone).replace(/\D/g, '');
  // datasigh manda "5511..." → remove 55 e remonta
  if (d.startsWith('55')) d = d.slice(2);
  if (d.length === 10) d = d.slice(0, 2) + '9' + d.slice(2); // insere 9 se faltar
  if (d.length !== 11) return null;
  return `+55${d}`;
}

/* ============ Datasigh ============ */
async function getAppointments(dateStr) {
  const url = `${DATASIGH_BASE_URL}/agendas/marcadas`;
  const params = { data: dsFormat(dateStr) };
  const headers = { Accept: 'application/json' };
  // IMPORTANTE: sem "Bearer"
  if (DATASIGH_API_KEY) headers.Authorization = DATASIGH_API_KEY;

  try {
    const { data } = await http.get(url, { params, headers });
    // shape: { agendas: [ ... ], datas: [...] }
    if (Array.isArray(data?.agendas)) return data.agendas;
    // fallback (caso a API um dia mude)
    if (Array.isArray(data)) return data;
    return [];
  } catch (err) {
    console.error('Datasigh error:', err?.response?.data || err.message);
    throw err;
  }
}

/* ============ TalkBI ============ */
function talkbiHeaders() {
  const h = { 'Content-Type': 'application/json', Accept: 'application/json' };
  if (TALKBI_API_KEY) h.Authorization = `Bearer ${TALKBI_API_KEY}`;
  return h;
}
function extractSubscribers(resp) {
  let d = resp;
  if (d && d.data !== undefined) d = d.data;
  if (d && !Array.isArray(d) && typeof d === 'object' && (d.user_ns || d.id || d.ns || d.uuid))
    return [d];
  if (Array.isArray(d)) return d;
  if (d && Array.isArray(d.items)) return d.items;
  if (d && d.data && Array.isArray(d.data)) return d.data;
  if (d && d.data && Array.isArray(d.data.items)) return d.data.items;
  return [];
}
async function resolveTalkBIUserNsByPhone(e164Phone) {
  const url = `${TALKBI_BASE_URL}/subscribers`;
  const { data } = await http.get(url, {
    headers: talkbiHeaders(),
    params: { phone: e164Phone, limit: 1, page: 1 }
  });
  const items = extractSubscribers(data);
  const s = items[0];
  return s ? (s.user_ns || s.ns || s.id || s.uuid || s.user_id || null) : null;
}
async function sendTalkBISubFlowByName(userNs, flowName, variables) {
  const url = `${TALKBI_BASE_URL}/subscriber/send-sub-flow-by-flow-name`;
  const payload = { user_ns: userNs, flow_name: flowName };
  if (variables && Object.keys(variables).length) payload.variables = variables;

  if (DRY_RUN) {
    console.log('[DRY_RUN] TalkBI payload →', JSON.stringify(payload, null, 2));
    return { dryRun: true, payload };
  }
  const { data } = await http.post(url, payload, { headers: talkbiHeaders() });
  return data;
}

/* ============ Mapping conforme seu JSON ============ */
function mapAppointmentToContact(ag) {
  const name = ag?.paciente?.nome || 'Paciente';
  const phone = normalizePhoneBR(ag?.paciente?.celular);
  const externalId = String(ag?.id ?? '');
  const horario = ag?.data; // "2025-10-15 14:30:00"
  const profissional = ag?.profissional?.nome;
  const unidade = ag?.unidade?.nome;

  return {
    phone,
    name,
    externalId,
    variables: { data_hora: horario, profissional, unidade }
  };
}

/* ============ Handler Vercel ============ */
module.exports = async (req, res) => {
  // Cron da Vercel usa GET. Vamos aceitar GET e POST.
  if (req.method !== 'GET' && req.method !== 'POST') {
    return res.status(405).json({ error: 'Method Not Allowed. Use GET or POST.' });
  }

  try {
    // GET: pega ?date=YYYY-MM-DD ou amanhã; POST: body.date ou amanhã
    const dateStr =
      (req.method === 'GET'
        ? (req.query && req.query.date) || tomorrowStr()
        : (req.body && req.body.date) || tomorrowStr());
    const appts = await getAppointments(dateStr);

    if (!appts.length) {
      return res.json({ date: dateStr, total: 0, sent: 0, skipped: 0, errors: [] });
    }

    const queue = new PQueue({ concurrency: CONCURRENCY, interval: INTERVAL_MS, intervalCap: INTERVAL_CAP });

    let sent = 0;
    let skipped = 0;
    const errors = [];

    const jobs = appts.map((ag) =>
      queue.add(async () => {
        const contact = mapAppointmentToContact(ag);
        if (!contact.phone) {
          skipped++;
          return { status: 'skipped', reason: 'missing_or_invalid_phone' };
        }
        try {
          const userNs = await resolveTalkBIUserNsByPhone(contact.phone);
          if (!userNs) {
            skipped++;
            errors.push({ agendamento: contact.externalId, error: 'subscriber_not_found_by_phone', phone: contact.phone });
            return { status: 'skipped', reason: 'subscriber_not_found' };
          }
          await sendTalkBISubFlowByName(userNs, TALKBI_FLOW_NAME, contact.variables);
          sent++;
          return { status: 'ok' };
        } catch (err) {
          const payload = err?.response?.data || err.message;
          errors.push({ agendamento: contact.externalId, error: payload });
          return { status: 'error', error: payload };
        }
      })
    );

    await Promise.all(jobs);
    return res.json({ date: dateStr, total: appts.length, sent, skipped, errors });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err?.response?.data || err.message });
  }
};
