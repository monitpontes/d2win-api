// src/services/telemetry.js
import mongoose from "mongoose";
import Device from "../models/device.js";
import BridgeStatus from "../models/bridgeStatus.js";
import { toBrazilISOFromUTC, brazilPartsFromUTC } from "../lib/time.js";

/** Coleções cruas (time-series) usadas via driver nativo */
const ACCEL = "telemetry_ts_accel";
const FREQ  = "telemetry_ts_freq_peaks";
const STAT  = "telemetry_ts_device_status"; // opcional, use se quiser histórico de status

/* =====================================================================
 * INSERÇÕES DE TELEMETRIA (com ts UTC e ts_br obrigatório)
 * ===================================================================*/

/**
 * Aceleração instantânea (ex.: eixo Z)
 * Campos gravados:
 *   ts (UTC Date), ts_br (string BR-3), date_br, hour_br
 *   meta: { company_id, bridge_id, device_id, stream: "accel:z" }
 *   value (m/s2), units, fw
 */
export async function insertAccel({
  company_id, bridge_id, device_id,
  ts, axis = "z", value, fw
}) {
  const tsUTC = ts ? new Date(ts) : new Date();
  const { ts_br, date_br, hour_br } = brazilPartsFromUTC(tsUTC);

  const doc = {
    ts: tsUTC,
    ts_br, date_br, hour_br,
    meta: { company_id, bridge_id, device_id, stream: `accel:${axis}` },
    value, units: "m/s2", fw
  };
  return mongoose.connection.db.collection(ACCEL).insertOne(doc);
}

/**
 * Resultado de FFT (picos) ou keep-alive
 * Campos gravados:
 *   ts (UTC), ts_br, date_br, hour_br
 *   meta: { company_id, bridge_id, device_id, stream: "freq:z" }
 *   status ("atividade_detectada" | "sem_atividade"), fs, n, peaks[], fw
 */
export async function insertFreqPeaks({
  company_id, bridge_id, device_id,
  ts, status, fs, n, peaks, fw
}) {
  const tsUTC = ts ? new Date(ts) : new Date();
  const { ts_br, date_br, hour_br } = brazilPartsFromUTC(tsUTC);

  const doc = {
    ts: tsUTC,
    ts_br, date_br, hour_br,
    meta: { company_id, bridge_id, device_id, stream: "freq:z" },
    status, fs, n, peaks, fw
  };
  return mongoose.connection.db.collection(FREQ).insertOne(doc);
}

/**
 * Status pontual do dispositivo (opcional, se quiser registrar histórico)
 * Campos gravados:
 *   ts (UTC), ts_br, date_br, hour_br
 *   meta: { company_id, bridge_id, device_id }
 *   status, rssi, battery_v
 */
export async function insertDeviceStatus({
  company_id, bridge_id, device_id,
  ts, status, rssi, battery_v
}) {
  const tsUTC = ts ? new Date(ts) : new Date();
  const { ts_br, date_br, hour_br } = brazilPartsFromUTC(tsUTC);

  const doc = {
    ts: tsUTC,
    ts_br, date_br, hour_br,
    meta: { company_id, bridge_id, device_id },
    status, rssi, battery_v
  };
  return mongoose.connection.db.collection(STAT).insertOne(doc);
}

/** Atualiza last_seen do device ao receber telemetria (opcional) */
export async function touchDeviceLastSeen(device_id, infos = undefined) {
  const set = { last_seen: new Date() };
  if (infos && typeof infos === "object") set.infos = infos;
  await Device.updateOne({ device_id }, { $set: set });
}

/* =====================================================================
 * HEARTBEAT / SNAPSHOT POR PONTE (UM DOCUMENTO POR bridge_id)
 * ===================================================================*/

/**
 * Regras de classificação do heartbeat:
 *  - active:   last_seen <= 90s
 *  - stale:    90s < last_seen <= 10min
 *  - offline:  > 10min ou null
 * Ajuste os thresholds conforme necessário.
 */
export const ACTIVE_MS  = 90 * 1000;
export const OFFLINE_MS = 10 * 60 * 1000;

function classify(lastSeen, nowMs = Date.now()) {
  if (!lastSeen) return "offline";
  const delta = nowMs - new Date(lastSeen).getTime();
  if (delta <= ACTIVE_MS)  return "active";
  if (delta <= OFFLINE_MS) return "stale";
  return "offline";
}

/**
 * Calcula e faz upsert do snapshot de UMA ponte
 * (coleção: bridge_device_status; 1 doc por ponte via unique index).
 */
export async function updateBridgeStatusFor(bridgeId, companyId) {
  const now = new Date();
  const nowMs = now.getTime();

  // busca devices dessa ponte/empresa
  const devices = await Device.find({ bridge_id: bridgeId, company_id: companyId })
                              .select("device_id last_seen infos")
                              .lean();

  // prepara linhas
  const rows = devices.map(d => {
    const status = classify(d.last_seen, nowMs);
    const ts_br = d.last_seen ? toBrazilISOFromUTC(new Date(d.last_seen)) : null;
    return {
      device_id: d.device_id,
      last_seen: d.last_seen || null,
      ts_br,
      ms_since: d.last_seen ? (nowMs - new Date(d.last_seen).getTime()) : null,
      status,
      rssi: d?.infos?.rssi ?? undefined,
      battery_v: d?.infos?.battery_v ?? undefined,
    };
  });

  // resumo da ponte
  const summary = {
    total:   rows.length,
    active:  rows.filter(r => r.status === "active").length,
    stale:   rows.filter(r => r.status === "stale").length,
    offline: rows.filter(r => r.status === "offline").length,
  };
  summary.status = summary.active > 0 ? "active" : (summary.stale > 0 ? "stale" : "offline");

  const { ts_br, date_br, hour_br } = brazilPartsFromUTC(now);

  // upsert (um documento por ponte)
  await BridgeStatus.findOneAndUpdate(
    { company_id: companyId, bridge_id: bridgeId },
    {
      $set: {
        updated_at: now,
        ts_br, date_br, hour_br,
        summary,
        devices: rows,
      },
    },
    { upsert: true, new: true, setDefaultsOnInsert: true }
  );

  return { ok: true, bridge_id: bridgeId, company_id: companyId, summary };
}

/** Varre todas as pontes existentes e atualiza os snapshots. */
export async function updateAllBridgeStatuses() {
  const pairs = await Device.aggregate([
    { $group: { _id: { company_id: "$company_id", bridge_id: "$bridge_id" } } }
  ]);

  const results = [];
  for (const p of pairs) {
    const { company_id, bridge_id } = p._id;
    const r = await updateBridgeStatusFor(bridge_id, company_id);
    results.push(r);
  }
  return results;
}

/* =====================================================================
 * "LATEST" – ÚLTIMOS VALORES POR EMPRESA / PONTE (p/ Dashboard & Bridge Page)
 * ===================================================================*/

/** Tenta casar tanto string quanto ObjectId (robusto para dados mistos) */
function idVariants(id) {
  const list = [String(id)];
  try { list.push(new mongoose.Types.ObjectId(id)); } catch {}
  return list;
}

/**
 * Agrupa por meta.device_id pegando o doc mais novo (por ts desc),
 * usando sort compatível com o índice e opt-in para spill to disk.
 * @param {string} collectionName
 * @param {object} matchQuery  // ex.: { "meta.company_id": {$in:[...]}, "meta.device_id": {$in:[...]} }
 * @returns Map<device_id(string), doc>
 */
async function latestPerDevice(collectionName, matchQuery) {
  const coll = mongoose.connection.db.collection(collectionName);

  // Descobre se o filtro é por empresa ou por ponte para montar sort + hint que batem com os índices
  const byCompany = Object.prototype.hasOwnProperty.call(matchQuery, "meta.company_id");
  const byBridge  = Object.prototype.hasOwnProperty.call(matchQuery, "meta.bridge_id");

  // Sort alinhado ao índice:
  //   company: { meta.company_id:1, meta.device_id:1, ts:-1 }
  //   bridge:  { meta.bridge_id:1,  meta.device_id:1, ts:-1 }
  const sortStage = byCompany
    ? { "meta.company_id": 1, "meta.device_id": 1, ts: -1 }
    : { "meta.bridge_id": 1,  "meta.device_id": 1, ts: -1 };

  // Hint do índice correspondente (se aplicável)
  let hint;
  if (byCompany) {
    hint = { "meta.company_id": 1, "meta.device_id": 1, ts: -1 };
  } else if (byBridge) {
    hint = { "meta.bridge_id": 1, "meta.device_id": 1, ts: -1 };
  }

  const pipeline = [
    { $match: matchQuery },
    { $sort: sortStage },
    { $group: { _id: "$meta.device_id", doc: { $first: "$$ROOT" } } },
  ];

  try {
    const cursor = hint
      ? coll.aggregate(pipeline, { allowDiskUse: true, hint })
      : coll.aggregate(pipeline, { allowDiskUse: true });

    const rows = await cursor.toArray();
    const map = new Map();
    for (const r of rows) map.set(String(r._id), r.doc);
    return map;
  } catch (err) {
    // fallback sem hint (alguns planos/versões rejeitam hint em aggregate)
    const rows = await coll.aggregate(pipeline, { allowDiskUse: true }).toArray();
    const map = new Map();
    for (const r of rows) map.set(String(r._id), r.doc);
    return map;
  }
}

/**
 * Últimos valores para TODOS os devices da empresa.
 * Retorna: { company_id, updated_at, devices: [ { device_id, bridge_id, modo_operacao, status, last_seen, accel, freq, ... } ] }
 */
export async function latestByCompany(companyId) {
  const variants = idVariants(companyId);

  // busca devices ativos da empresa
  const devs = await Device.find({ company_id: { $in: variants }, isActive: { $ne: false } })
    .select("device_id bridge_id company_id modo_operacao last_seen params_current isActive")
    .lean();

  // Na sua TS você grava meta.device_id com o TEXTUAL device_id
  const deviceCodes = devs.map(d => String(d.device_id));

  // Query por empresa + device_id textual
  const accelMap = await latestPerDevice(ACCEL, { "meta.company_id": { $in: variants }, "meta.device_id": { $in: deviceCodes } });
  const freqMap  = await latestPerDevice(FREQ,  { "meta.company_id": { $in: variants }, "meta.device_id": { $in: deviceCodes } });

  const now = Date.now();
  const items = devs.map(d => {
    const k = String(d.device_id);
    const accel = accelMap.get(k) || null;
    const freq  = freqMap.get(k)  || null;
    const status = classify(d.last_seen, now);

    return {
      device_id: d.device_id,
      bridge_id: d.bridge_id,
      company_id: d.company_id,
      modo_operacao: d.modo_operacao || "aceleracao",
      last_seen: d.last_seen || null,
      status,
      params_current: d.params_current || {},
      isActive: d.isActive !== false,

      // aceleração (ajuste os campos conforme seus docs)
      accel: accel
        ? {
            ts: accel.ts,
            // campos comuns—ajuste conforme você grava:
            value: accel.value ?? null,           // m/s2 instantâneo (se usa stream accel:z)
            rms:   accel.rms   ?? accel?.metrics?.rms ?? null,
            ax:    accel.ax    ?? null,
            ay:    accel.ay    ?? null,
            az:    accel.az    ?? null,
          }
        : null,

      // frequência (picos/dominante)
      freq: freq
        ? {
            ts: freq.ts,
            status: freq.status ?? null,          // "atividade_detectada" | "sem_atividade"
            fs: freq.fs ?? null,
            n:  freq.n ?? null,
            peak:     freq.peak     ?? freq?.metrics?.peak ?? null,
            dom_freq: freq.dom_freq ?? freq?.metrics?.dom_freq ?? null,
            peaks:    freq.peaks    ?? freq?.metrics?.peaks ?? null,
          }
        : null,
    };
  });

  return {
    company_id: companyId,
    updated_at: new Date(),
    devices: items,
  };
}

/**
 * Últimos valores para TODOS os devices de uma ponte.
 * Retorna: { bridge_id, company_ids, updated_at, devices: [...] }
 */
export async function latestByBridge(bridgeId) {
  const variants = idVariants(bridgeId);

  const devs = await Device.find({ bridge_id: { $in: variants }, isActive: { $ne: false } })
    .select("device_id bridge_id company_id modo_operacao last_seen params_current isActive")
    .lean();

  if (devs.length === 0) {
    return { bridge_id: bridgeId, company_ids: [], updated_at: new Date(), devices: [] };
  }

  const companyIds = [...new Set(devs.map(d => String(d.company_id)))];
  const deviceCodes = devs.map(d => String(d.device_id));

  // match por bridge_id + device_id textual
  const accelMap = await latestPerDevice(ACCEL, { "meta.bridge_id": { $in: variants }, "meta.device_id": { $in: deviceCodes } });
  const freqMap  = await latestPerDevice(FREQ,  { "meta.bridge_id": { $in: variants }, "meta.device_id": { $in: deviceCodes } });

  const now = Date.now();
  const items = devs.map(d => {
    const k = String(d.device_id);
    const accel = accelMap.get(k) || null;
    const freq  = freqMap.get(k)  || null;
    const status = classify(d.last_seen, now);

    return {
      device_id: d.device_id,
      bridge_id: d.bridge_id,
      company_id: d.company_id,
      modo_operacao: d.modo_operacao || "aceleracao",
      last_seen: d.last_seen || null,
      status,
      params_current: d.params_current || {},
      isActive: d.isActive !== false,
      accel: accel
        ? {
            ts: accel.ts,
            value: accel.value ?? null,
            rms:   accel.rms   ?? accel?.metrics?.rms ?? null,
            ax:    accel.ax    ?? null,
            ay:    accel.ay    ?? null,
            az:    accel.az    ?? null,
          }
        : null,
      freq: freq
        ? {
            ts: freq.ts,
            status: freq.status ?? null,
            fs: freq.fs ?? null,
            n:  freq.n ?? null,
            peak:     freq.peak     ?? freq?.metrics?.peak ?? null,
            dom_freq: freq.dom_freq ?? freq?.metrics?.dom_freq ?? null,
            peaks:    freq.peaks    ?? freq?.metrics?.peaks ?? null,
          }
        : null,
    };
  });

  return {
    bridge_id: bridgeId,
    company_ids: companyIds,
    updated_at: new Date(),
    devices: items,
  };
}

/* =====================================================================
 * DICAS DE ÍNDICE (adicione nos schemas das TS para performance)
 * =====================================================================

Em cada coleção ts* (se você tiver Schema/Model), crie índices:

schema.index({ "meta.company_id": 1, "meta.device_id": 1, ts: -1 });
schema.index({ "meta.bridge_id": 1,  "meta.device_id": 1, ts: -1 });

Se usa as coleções só via driver (como aqui), crie-os via shell/CLI:
db.telemetry_ts_accel.createIndex({ "meta.company_id": 1, "meta.device_id": 1, ts: -1 })
db.telemetry_ts_accel.createIndex({ "meta.bridge_id": 1,  "meta.device_id": 1, ts: -1 })
db.telemetry_ts_freq_peaks.createIndex({ "meta.company_id": 1, "meta.device_id": 1, ts: -1 })
db.telemetry_ts_freq_peaks.createIndex({ "meta.bridge_id": 1,  "meta.device_id": 1, ts: -1 })

===================================================================== */
