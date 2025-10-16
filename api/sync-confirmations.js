// /api/sync-confirmations.js
require('dotenv').config();
const axios = require('axios');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const tz = require('dayjs/plugin/timezone');
// p-queue é ESM → import dinâmico dentro do handler

dayjs.extend(utc);
dayjs.extend(tz);

// ===== Timezone (Vercel reserva TZ; usamos APP_TZ e sanitizamos valores tipo ":UTC") =====
const APP_TZ = process.env.APP_TZ || '';
const RAW_TZ = APP_TZ || process.env.TZ || 'America/Sao_Paulo';
const TIMEZONE = (RAW_TZ || '').replace(/^:/, '') || 'America/Sao_Paulo';

// ===== Limites da fila =====
const CONCURRENCY = Number(process.env.CONCURRENCY || 4);
const INTERVAL_MS = Number(process.env.INTERVAL_MS || 1000);
const INTERVAL_CAP = Number(process.env.INTERVAL_CAP || 8);

// ===== Datasigh (SEM Bearer) =====
const DATASIGH_BASE_URL = process.env.DATASIGH_BASE_URL || 'https://ws.datasigh.com.br/api/integracao/v1';
const DATASIGH_API_KEY = process.env.DATASIGH_API_KEY || ''; // integration_hash:client_hash (sem "Bearer")
const DATASIGH_DATE_FORMAT = process.env.DATASIGH_DATE_FORMAT || 'DD/MM/YYYY';

// ===== TalkBI =====
const TALKBI_BASE_URL = process.env.TALKBI_BASE_URL || 'https://chat.talkbi.com.br/api';
const TALKBI_API_KEY = process.env.TALKBI_API_KEY || '';
const TALKBI_FLOW_NAME = process.env.TALKBI_FLOW_NAME || '';

const http = axios.create({ timeout: 15000 });

// ===== Utils =====
const isDry = () => String(process.env.DRY_RUN || 'false').toLowerCase() === 'true';

function tomorrowStr() {
  return dayjs().tz(TIMEZONE).add(1, 'day').format('YYYY-MM-DD');
}
function dsFormat(dateLike) {
  return dayjs(dateLike).tz(TIMEZONE).format(DATASIGH_DATE_FORMAT);
}
function normalizePhoneBR(phone) {
  if (!phone) return null;
  let d = String(phone).replace(/\D/g, '');
  if (d.startsWith('55')) d = d.slice(2);
  if (d.length === 10) d = d.slice(0, 2) + '9' + d.slice(2);
  if (d.length !== 11) return null;
  return `+55${d}`;
}
function fromAnyDate(s) {
  if (!s) return tomorrowStr();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;              // YYYY-MM-DD
  if (/^\d{2}\/\d{2}\/\d{4}$/.test(s)) {                    // DD/MM/YYYY
    const [d,m,y] = s.split('/'); return `${y}-${m}-${d}`;
  }
  return tomorrowStr();
}

// ===== Gera variações do mesmo telefone para tentar achar no TalkBI =====
function phoneVariants(e164Phone) {
  const set = new Set();
  const only = String(e164Phone || '').replace(/\D/g, '');

  // bases
  set.add(`+${only}`);
  set.add(only);

  // sem 55
  if (only.startsWith('55')) {
    const sem55 = only.slice(2);
    set.add(sem55);
    set.add(`+${sem55}`);
  }

  // se 55 + DDD + 9 + 8 dígitos (13 dígitos total), tente sem o 9
  if (only.startsWith('55') && only.length === 13) {
    const ddd = only.slice(2, 4);
    const nove = only.slice(4, 5);
    const resto = only.slice(5);
    if (nove === '9') {
      const sem9 = `55${ddd}${resto}`; // 12 dígitos
      set.add(sem9);
      set.add(`+${sem9}`);
    }
  }

  // se 55 + DDD + 8 dígitos (12), tente com o 9
  if (only.startsWith('55') && only.length === 12) {
    const ddd = only.slice(2, 4);
    const resto = only.slice(4);
    const com9 = `55${ddd}9${resto}`; // 13 dígitos
    set.add(com9);
    set.add(`+${com9}`);
  }

  return Array.from(set);
}

// ===== Datasigh =====
async function getAppointments(dateStr) {
  const url = `${DATASIGH_BASE_URL}/agendas/marcadas`;
  const params = { data: dsFormat(dateStr) };
  const headers = { Accept: 'application/json' };
  if (DATASIGH_API_KEY) headers.Authorization = DATASIGH_API_KEY; // << SEM Bearer

  try {
    const { data } = await http.get(url, { params, headers });
    // oficial: { agendas: [...], datas: [...] }
    if (Array.isArray(data?.agendas)) return data.agendas;
    if (Array.isArray(data)) return data;
    return [];
  } catch (err) {
    const out = {
      message: err?.message,
      axios: {
        status: err?.response?.status,
        data:   err?.response?.data,
        url:    err?.config?.url,
        params: err?.config?.params,
        method: err?.config?.method
      }
    };
    console.error('Datasigh error:', out);
    throw err;
  }
}

// ===== TalkBI =====
function talkbiHeaders() {
  const h = { 'Content-Type': 'application/json', Accept: 'application/json' };
  if (TALKBI_API_KEY) h.Authorization = `Bearer ${TALKBI_API_KEY}`;
  return h;
}
function extractSubscribers(resp) {
  let d = resp;
  if (d && d.data !== undefined) d = d.data;
  if (d && !Array.isArray(d) && typeof d === 'object' && (d.user_ns || d.id || d.ns || d.uuid)) return [d];
  if (Array.isArray(d)) return d;
  if (d && Array.isArray(d.items)) return d.items;
  if (d && d.data && Array.isArray(d.data)) return d.data;
  if (d && d.data && Array.isArray(d.data.items)) return d.data.items;
  return [];
}

// >>> alterado: tenta múltiplas variações do telefone
async function resolveTalkBIUserNsByPhone(e164Phone) {
  const url = `${TALKBI_BASE_URL}/subscribers`;
  const variants = phoneVariants(e164Phone);

  for (const phone of variants) {
    try {
      const { data } = await http.get(url, {
        headers: talkbiHeaders(),
        params: { phone, limit: 1, page: 1 }
      });
      const items = extractSubscribers(data);
      const s = items[0];
      if (s) return s.user_ns || s.ns || s.id || s.uuid || s.user_id || null;
    } catch {
      // ignora e tenta a próxima variação
    }
  }
  return null;
}

async function sendTalkBISubFlowByName(userNs, flowName, variables) {
  const url = `${TALKBI_BASE_URL}/subscriber/send-sub-flow-by-flow-name`;
  const payload = { user_ns: userNs, flow_name: flowName };
  if (variables && Object.keys(variables).length) payload.variables = variables;

  if (isDry()) {
    console.log('[DRY_RUN] TalkBI payload →', JSON.stringify(payload, null, 2));
    return { dryRun: true, payload };
  }
  const { data } = await http.post(url, payload, { headers: talkbiHeaders() });
  return data;
}

// ===== Mapping (Datasigh JSON) =====
function mapAppointmentToContact(ag) {
  const name = ag?.paciente?.nome || 'Paciente';
  const phone = normalizePhoneBR(ag?.paciente?.celular);
  const externalId = String(ag?.id ?? '');
  const horario = ag?.data;
  const profissional = ag?.profissional?.nome;
  const unidade = ag?.unidade?.nome;
  return { phone, name, externalId, variables: { data_hora: horario, profissional, unidade } };
}

// ===== Handler (Vercel) =====
module.exports = async (req, res) => {
  if (req.method !== 'GET' && req.method !== 'POST') {
    return res.status(405).json({ error: 'Method Not Allowed. Use GET or POST.' });
  }

  // Override de DRY_RUN via query/body (?dry=true/false)
  const dryParam = (req.method === 'GET' ? req.query?.dry : req.body?.dry);
  if (typeof dryParam === 'string') process.env.DRY_RUN = String(dryParam);

  try {
    // p-queue v8 é ESM → import dinâmico aqui
    const { default: PQueue } = await import('p-queue');

    // Data de entrada
    const dateStr = req.method === 'GET'
      ? fromAnyDate(req.query?.date)
      : (req.body?.date || tomorrowStr());

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
            errors.push({
              agendamento: contact.externalId,
              error: 'subscriber_not_found_by_phone',
              phone: contact.phone
            });
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
    const out = {
      message: err?.message,
      axios: {
        status: err?.response?.status,
        data:   err?.response?.data,
        url:    err?.config?.url,
        params: err?.config?.params,
        method: err?.config?.method
      }
    };
    console.error('sync-confirmations error:', out);
    return res.status(500).json({ error: out });
  }
};
