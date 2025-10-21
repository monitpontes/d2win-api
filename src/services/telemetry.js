// src/services/telemetry.js
import mongoose from "mongoose";
import Device from "../models/device.js";
import BridgeStatus from "../models/bridgeStatus.js";
import { toBrazilISOFromUTC, brazilPartsFromUTC } from "../lib/time.js";

/** ====================================================================
 * Coleções (time-series)
 * ==================================================================== */
const ACCEL = "telemetry_ts_accel";
const FREQ  = "telemetry_ts_freq_peaks";
const STAT  = "telemetry_ts_device_status"; // opcional
const BRIDGE_LIMITS = "bridge_limits";

/** ====================================================================
 * 🔧 Proteções e limites
 * ==================================================================== */
const HISTORY_DEFAULT_N = Number(process.env.HISTORY_DEFAULT_N || 10);  // últimos 10 para history
const DEVICE_HARD_CAP   = Number(process.env.DEVICE_HARD_CAP   || 2000);
const QUERY_MAX_MS      = Number(process.env.QUERY_MAX_MS      || 3000);

// TTL do cache de limites (mesmo com modo FIXED, mantemos estrutura)
const LIMITS_TTL_MS     = Number(process.env.BRIDGE_LIMITS_TTL_MS || (24 * 60 * 60 * 1000));

/** Projeções mínimas → menos bytes trafegando no pipe */
const ACCEL_PROJECT_MIN = {
  _id: 0,
  ts: 1,
  value: 1, rms: 1, ax: 1, ay: 1, az: 1, severity: 1,
  // ✅ incluir metrics.* (espelhar o FREQ)
  "metrics.value": 1, "metrics.rms": 1, "metrics.ax": 1, "metrics.ay": 1, "metrics.az": 1,
  "meta.device_id": 1,
  "meta.severity": 1,
  fw: 1, units: 1,
};

const FREQ_PROJECT_MIN = {
  _id: 0,
  ts: 1,
  status: 1, fs: 1, n: 1,
  peak: 1, dom_freq: 1, peaks: 1, severity: 1,
  "metrics.peak": 1, "metrics.dom_freq": 1, "metrics.peaks": 1,
  "meta.device_id": 1,
  "meta.severity": 1,
  fw: 1,
};

/* =====================================================================
 * 🔒 Cache de limites por ponte (em memória)
 * ===================================================================*/
const limitsCache = new Map(); // bridgeId -> { at, v }
function numberOrNull(x, dflt = null) {
  const n = Number(x);
  return Number.isFinite(n) ? n : dflt;
}

/* =====================================================================
 * ⚙️ LIMITES "NA MÃO" (toggle)
 *  - Coloque USE_FIXED_LIMITS = true para usar os valores abaixo.
 *  - Para voltar ao banco de dados, mude para false.
 * ===================================================================*/
const USE_FIXED_LIMITS = true;

const FIXED_LIMITS_DEFAULT = {
  freq_alert: 3.7,
  freq_critical: 7,
  accel_alert: 12,
  accel_critical: 20,
};

// opcional: overrides por ponte (key = bridge_id string)
const FIXED_LIMITS_BY_BRIDGE = {
  // "BRIDGE_OBJECT_ID_HERE": { freq_alert: 3.7, freq_critical: 7, accel_alert: 10, accel_critical: 20 }
};

async function getBridgeLimitsCached(bridgeId) {
  const key = String(bridgeId);
  const now = Date.now();

  if (USE_FIXED_LIMITS) {
    const raw = FIXED_LIMITS_BY_BRIDGE[key] || FIXED_LIMITS_DEFAULT;
    const v = {
      freq_alert:    numberOrNull(raw.freq_alert, 3.7),
      freq_critical: numberOrNull(raw.freq_critical, 7),
      accel_alert:   numberOrNull(raw.accel_alert, 12),
      accel_critical:numberOrNull(raw.accel_critical, 20),
      _source: "fixed",
    };
    limitsCache.set(key, { at: now, v });
    return v;
  }

  const hit = limitsCache.get(key);
  if (hit && (now - hit.at) < LIMITS_TTL_MS) return hit.v;

  const db = mongoose.connection.db;
  const doc = await db.collection(BRIDGE_LIMITS).findOne(
    { bridge_id: new mongoose.Types.ObjectId(bridgeId) },
    { projection: { _id: 0, freq_alert: 1, freq_critical: 1, accel_alert: 1, accel_critical: 1, updatedAt: 1 } }
  );

  const v = {
    freq_alert:    numberOrNull(doc?.freq_alert, 3.7),
    freq_critical: numberOrNull(doc?.freq_critical, 7),
    accel_alert:   numberOrNull(doc?.accel_alert, 10),
    accel_critical:numberOrNull(doc?.accel_critical, 20),
    _limitsUpdatedAt: doc?.updatedAt || null,
    _source: doc ? "db" : "default",
  };
  limitsCache.set(key, { at: now, v });
  return v;
}

// (Opcional) watcher — no modo FIXED não muda nada, mas deixamos pronto
export function startBridgeLimitsWatcher() {
  try {
    const db = mongoose.connection.db;
    const cs = db.collection(BRIDGE_LIMITS).watch([
      { $match: { operationType: { $in: ["insert", "update", "replace", "delete"] } } }
    ]);
    cs.on("change", (ch) => {
      const bridgeId = ch.fullDocument?.bridge_id || ch.updateDescription?.updatedFields?.bridge_id;
      if (bridgeId) limitsCache.delete(String(bridgeId));
      else limitsCache.clear();
    });
  } catch {
    // ok em ambientes sem changeStream
  }
}

/* =====================================================================
 * Helpers de severidade (sempre a partir do VALOR ATUAL)
 * ===================================================================*/
function severityFromValue(v, warn, crit) {
  // regra confirmada: sem valor/sem atividade => NORMAL
  if (v == null || !Number.isFinite(v)) return "normal";
  if (v > crit)  return "critical";
  if (v > warn)  return "warning";
  return "normal";
}

function rmsFromAxes(ax, ay, az) {
  const x = numberOrNull(ax, 0), y = numberOrNull(ay, 0), z = numberOrNull(az, 0);
  return Math.sqrt(x*x + y*y + z*z);
}

/* =====================================================================
 * INSERÇÕES DE TELEMETRIA (severidade calculada aqui)
 * ===================================================================*/

// Aceleração
export async function insertAccel({
  company_id, bridge_id, device_id,
  ts, axis = "z", value, rms, ax, ay, az, fw,
}) {
  const tsUTC = ts ? new Date(ts) : new Date();
  const { ts_br, date_br, hour_br } = brazilPartsFromUTC(tsUTC);

  const limits = await getBridgeLimitsCached(bridge_id);

  // valor efetivo (preferência: rms > value > vetor de eixos)
  const eff = numberOrNull(rms ?? value, null) ?? rmsFromAxes(ax, ay, az);
  const sev = severityFromValue(eff, limits.accel_alert, limits.accel_critical);

  const doc = {
    ts: tsUTC,
    ts_br, date_br, hour_br,
    meta: { company_id, bridge_id, device_id, stream: `accel:${axis}`, severity: sev },
    value: numberOrNull(value, undefined),
    rms:   numberOrNull(rms,   undefined),
    ax:    numberOrNull(ax,    undefined),
    ay:    numberOrNull(ay,    undefined),
    az:    numberOrNull(az,    undefined),
    units: "m/s2",
    fw,
    severity: sev,
  };
  return mongoose.connection.db.collection(ACCEL).insertOne(doc);
}

// Frequência (picos/keep-alive)
export async function insertFreqPeaks({
  company_id, bridge_id, device_id,
  ts, status, fs, n, peaks, dom_freq, peak, fw,
}) {
  const tsUTC = ts ? new Date(ts) : new Date();
  const { ts_br, date_br, hour_br } = brazilPartsFromUTC(tsUTC);

  const limits = await getBridgeLimitsCached(bridge_id);

  // "sem_atividade" => tratamos efetivo como null => NORMAL
  const isIdle = status === "sem_atividade";

  // valor efetivo (dom_freq > peak > peaks[0].f)
  const p0 = Array.isArray(peaks) && peaks.length ? peaks[0] : null;
  const fEff = isIdle
    ? null
    : ( numberOrNull(dom_freq, null)
        ?? numberOrNull(peak, null)
        ?? numberOrNull(p0?.f ?? p0?.freq ?? p0?.x, null) );

  const sev = severityFromValue(fEff, limits.freq_alert, limits.freq_critical);

  const doc = {
    ts: tsUTC,
    ts_br, date_br, hour_br,
    meta: { company_id, bridge_id, device_id, stream: "freq:z", severity: sev },
    status: isIdle ? "sem_atividade" : (status ?? null),
    fs: numberOrNull(fs, undefined),
    n:  numberOrNull(n,  undefined),
    peaks,
    fw,
    dom_freq: numberOrNull(dom_freq, undefined),
    peak:     numberOrNull(peak,     undefined),
    severity: sev,
  };
  return mongoose.connection.db.collection(FREQ).insertOne(doc);
}

// Status do dispositivo (histórico opcional)
export async function insertDeviceStatus({ company_id, bridge_id, device_id, ts, status, rssi, battery_v }) {
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

/* =====================================================================
 * SNAPSHOT POR PONTE
 * ===================================================================*/

export const ACTIVE_MS  = 90 * 1000;
export const OFFLINE_MS = 10 * 60 * 1000;

function classify(lastSeen, nowMs = Date.now()) {
  if (!lastSeen) return "offline";
  const delta = nowMs - new Date(lastSeen).getTime();
  if (delta <= ACTIVE_MS)  return "active";
  if (delta <= OFFLINE_MS) return "stale";
  return "offline";
}

export async function updateBridgeStatusFor(bridgeId, companyId) {
  const now = new Date();
  const nowMs = now.getTime();

  const devices = await Device.find({ bridge_id: bridgeId, company_id: companyId })
    .select("device_id last_seen infos")
    .lean();

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

  const summary = {
    total:   rows.length,
    active:  rows.filter(r => r.status === "active").length,
    stale:   rows.filter(r => r.status === "stale").length,
    offline: rows.filter(r => r.status === "offline").length,
  };
  summary.status = summary.active > 0 ? "active" : (summary.stale > 0 ? "stale" : "offline");

  const { ts_br, date_br, hour_br } = brazilPartsFromUTC(now);

  await BridgeStatus.findOneAndUpdate(
    { company_id: companyId, bridge_id: bridgeId },
    { $set: { updated_at: now, ts_br, date_br, hour_br, summary, devices: rows } },
    { upsert: true, new: true, setDefaultsOnInsert: true }
  );

  return { ok: true, bridge_id: bridgeId, company_id: companyId, summary };
}

export async function updateAllBridgeStatuses() {
  const pairs = await Device.aggregate([{ $group: { _id: { company_id: "$company_id", bridge_id: "$bridge_id" } } }]);
  const results = [];
  for (const p of pairs) {
    const { company_id, bridge_id } = p._id;
    const r = await updateBridgeStatusFor(bridge_id, company_id);
    results.push(r);
  }
  return results;
}

/* =====================================================================
 * LATEST (Dashboard / Bridge Page)
 * ===================================================================*/

function idVariants(id) {
  const list = [String(id)];
  try { list.push(new mongoose.Types.ObjectId(id)); } catch {}
  return list;
}
function getHintFor(matchQuery) {
  if (matchQuery["meta.bridge_id"])  return { "meta.bridge_id": 1,  "meta.device_id": 1, ts: -1 };
  if (matchQuery["meta.company_id"]) return { "meta.company_id": 1, "meta.device_id": 1, ts: -1 };
  return undefined;
}

/** Normaliza doc de ACCEL e recalcula severidade pelo VALOR */
function mapAccelDoc(d, limits) {
  if (!d) return null;
  // ✅ agora com fallbacks para metrics.*
  const rms = numberOrNull(d.rms ?? d?.metrics?.rms, null);
  const val = numberOrNull(d.value ?? d?.metrics?.value, null);
  const ax  = numberOrNull(d.ax ?? d?.metrics?.ax, null);
  const ay  = numberOrNull(d.ay ?? d?.metrics?.ay, null);
  const az  = numberOrNull(d.az ?? d?.metrics?.az, null);
  const eff = (rms ?? val ?? ((ax!=null||ay!=null||az!=null) ? rmsFromAxes(ax,ay,az) : null));
  const sev = severityFromValue(eff, limits.accel_alert, limits.accel_critical);
  return { ts: d.ts, value: val, rms, ax, ay, az, severity: sev };
}

/** Normaliza doc de FREQ e recalcula severidade pelo VALOR */
function mapFreqDoc(d, limits) {
  if (!d) return null;
  const isIdle = d.status === "sem_atividade";
  const dom  = numberOrNull(d.dom_freq ?? d?.metrics?.dom_freq, null);
  const peak = numberOrNull(d.peak     ?? d?.metrics?.peak,     null);
  const p0   = Array.isArray(d.peaks ?? d?.metrics?.peaks) && (d.peaks ?? d?.metrics?.peaks).length
    ? (d.peaks ?? d?.metrics?.peaks)[0] : null;
  const fEff = isIdle ? null : (dom ?? peak ?? numberOrNull(p0?.f ?? p0?.freq ?? p0?.x, null));
  const sev  = severityFromValue(fEff, limits.freq_alert, limits.freq_critical);

  return {
    ts: d.ts,
    status: isIdle ? "sem_atividade" : (d.status ?? null),
    fs: numberOrNull(d.fs, null),
    n:  numberOrNull(d.n,  null),
    peak,
    dom_freq: dom,
    peaks: d.peaks ?? d?.metrics?.peaks ?? null,
    severity: sev,
  };
}

async function latestPerDevice(collectionName, matchQuery) {
  const coll = mongoose.connection.db.collection(collectionName);
  const hint = getHintFor(matchQuery);
  const projectMin = (collectionName === ACCEL ? ACCEL_PROJECT_MIN : FREQ_PROJECT_MIN);

  const pipeline = [
    { $match: matchQuery },
    { $sort: { "meta.device_id": 1, ts: -1 } },
    { $group: { _id: "$meta.device_id", doc: { $first: "$$ROOT" } } },
    { $replaceRoot: { newRoot: "$doc" } },
    { $project: projectMin },
    { $limit: DEVICE_HARD_CAP },
  ];

  try {
    const rows = await coll.aggregate(pipeline, { allowDiskUse: false, hint, maxTimeMS: QUERY_MAX_MS }).toArray();
    const map = new Map();
    for (const r of rows) map.set(String(r?.meta?.device_id), r);
    return map;
  } catch {
    return new Map();
  }
}

export async function latestByCompany(companyId) {
  const variants = idVariants(companyId);

  const devs = await Device.find({ company_id: { $in: variants }, isActive: { $ne: false } })
    .select("device_id bridge_id company_id modo_operacao last_seen params_current isActive")
    .lean();

  const deviceCodes = devs.map(d => String(d.device_id));

  const accelMap = await latestPerDevice(ACCEL, { "meta.company_id": { $in: variants }, "meta.device_id": { $in: deviceCodes } });
  const freqMap  = await latestPerDevice(FREQ,  { "meta.company_id": { $in: variants }, "meta.device_id": { $in: deviceCodes } });

  const now = Date.now();
  const items = [];
  for (const d of devs) {
    const k = String(d.device_id);
    const accelRaw = accelMap.get(k) || null;
    const freqRaw  = freqMap.get(k)  || null;
    const status = classify(d.last_seen, now);

    // limites por ponte (cacheado) — um hit por device (mesma ponte → cache hit)
    const limits = await getBridgeLimitsCached(d.bridge_id);

    const accel = accelRaw ? mapAccelDoc(accelRaw, limits) : null;
    const freq  = freqRaw  ? mapFreqDoc(freqRaw, limits)   : null;

    items.push({
      device_id: d.device_id,
      bridge_id: d.bridge_id,
      company_id: d.company_id,
      modo_operacao: d.modo_operacao || "aceleracao",
      last_seen: d.last_seen || null,
      status,
      params_current: d.params_current || {},
      isActive: d.isActive !== false,
      accel,
      freq,
    });
  }

  return { company_id: companyId, updated_at: new Date(), devices: items };
}

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

  const accelMap = await latestPerDevice(ACCEL, { "meta.bridge_id": { $in: variants }, "meta.device_id": { $in: deviceCodes } });
  const freqMap  = await latestPerDevice(FREQ,  { "meta.bridge_id": { $in: variants }, "meta.device_id": { $in: deviceCodes } });

  const now = Date.now();
  const limits = await getBridgeLimitsCached(bridgeId); // uma vez por ponte
  const items = devs.map(d => {
    const k = String(d.device_id);
    const accel = mapAccelDoc(accelMap.get(k) || null, limits);
    const freq  = mapFreqDoc(freqMap.get(k)  || null, limits);
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
      accel,
      freq,
    };
  });

  return { bridge_id: bridgeId, company_ids: companyIds, updated_at: new Date(), devices: items };
}

/* =====================================================================
 * HISTORY por ponte (N últimos por device/stream)
 * ===================================================================*/

// cap de ids de device
function clampDeviceIds(deviceIds = []) {
  if (!Array.isArray(deviceIds)) return [];
  if (deviceIds.length > DEVICE_HARD_CAP) return deviceIds.slice(0, DEVICE_HARD_CAP);
  return deviceIds;
}

// pega os N mais recentes por device, com fallbacks progressivos
async function lastNPerDevice(collectionName, matchQuery, limit = 10, deviceIds = []) {
  const coll = mongoose.connection.db.collection(collectionName);
  if (Array.isArray(deviceIds)) deviceIds = clampDeviceIds(deviceIds);
  const hint = getHintFor(matchQuery);

  // 1) $topN (MongoDB 5.2+)
  const pipelineTopN = [
    { $match: matchQuery },
    { $group: {
        _id: "$meta.device_id",
        docs: { $topN: { sortBy: { ts: -1 }, output: "$$ROOT", n: Math.min(Number(limit || HISTORY_DEFAULT_N), HISTORY_DEFAULT_N) } }
    }},
    { $project: { _id: 0, device_id: "$_id", docs: { $reverseArray: "$docs" } } },
    { $limit: DEVICE_HARD_CAP }
  ];

  try {
    const arr = await coll.aggregate(pipelineTopN, { allowDiskUse: false, hint, maxTimeMS: QUERY_MAX_MS }).toArray();
    const map = new Map();
    for (const r of arr) {
      r.docs = r.docs.map(d => {
        if (collectionName === ACCEL) {
          const { ts, value, rms, ax, ay, az, severity, metrics, meta, fw, units } = d;
          return {
            ts,
            value: numberOrNull(value ?? metrics?.value, null),
            rms:   numberOrNull(rms   ?? metrics?.rms,   null),
            ax:    numberOrNull(ax    ?? metrics?.ax,    null),
            ay:    numberOrNull(ay    ?? metrics?.ay,    null),
            az:    numberOrNull(az    ?? metrics?.az,    null),
            severity: severity ?? meta?.severity ?? "normal",
            meta: { device_id: meta?.device_id, severity: meta?.severity },
            fw, units
          };
        } else {
          const { ts, status, fs, n, peak, dom_freq, peaks, severity, metrics, meta, fw } = d;
          return {
            ts, status, fs, n,
            peak: numberOrNull(peak ?? metrics?.peak, null),
            dom_freq: numberOrNull(dom_freq ?? metrics?.dom_freq, null),
            peaks: peaks ?? metrics?.peaks ?? null,
            severity: severity ?? meta?.severity ?? "normal",
            meta: { device_id: meta?.device_id, severity: meta?.severity },
            fw
          };
        }
      });
      map.set(String(r.device_id), r.docs);
    }
    return map;
  } catch {
    // 2) janela
    const pipelineWindow = [
      { $match: matchQuery },
      { $setWindowFields: { partitionBy: "$meta.device_id", sortBy: { ts: -1 }, output: { rk: { $documentNumber: {} } } } },
      { $match: { rk: { $lte: Math.min(Number(limit || HISTORY_DEFAULT_N), HISTORY_DEFAULT_N) } } },
      { $sort: { "meta.device_id": 1, ts: 1 } },
      { $group: { _id: "$meta.device_id", docs: { $push: "$$ROOT" } } },
      { $project: { _id: 0, device_id: "$_id", docs: 1 } },
      { $limit: DEVICE_HARD_CAP }
    ];

    try {
      const arr = await coll.aggregate(pipelineWindow, { allowDiskUse: false, hint, maxTimeMS: QUERY_MAX_MS }).toArray();
      const map = new Map();
      for (const r of arr) {
        r.docs = r.docs.map(d => {
          if (collectionName === ACCEL) {
            const { ts, value, rms, ax, ay, az, severity, metrics, meta, fw, units } = d;
            return {
              ts,
              value: numberOrNull(value ?? metrics?.value, null),
              rms:   numberOrNull(rms   ?? metrics?.rms,   null),
              ax:    numberOrNull(ax    ?? metrics?.ax,    null),
              ay:    numberOrNull(ay    ?? metrics?.ay,    null),
              az:    numberOrNull(az    ?? metrics?.az,    null),
              severity: severity ?? meta?.severity ?? "normal",
              meta: { device_id: meta?.device_id, severity: meta?.severity },
              fw, units
            };
          } else {
            const { ts, status, fs, n, peak, dom_freq, peaks, severity, metrics, meta, fw } = d;
            return {
              ts, status, fs, n,
              peak: numberOrNull(peak ?? metrics?.peak, null),
              dom_freq: numberOrNull(dom_freq ?? metrics?.dom_freq, null),
              peaks: peaks ?? metrics?.peaks ?? null,
              severity: severity ?? meta?.severity ?? "normal",
              meta: { device_id: meta?.device_id, severity: meta?.severity },
              fw
            };
          }
        });
        map.set(String(r.device_id), r.docs);
      }
      return map;
    } catch {
      // 3) fallback final por device
      const ids = deviceIds.length ? deviceIds : await coll.distinct("meta.device_id", matchQuery);
      const map = new Map();
      for (const id of clampDeviceIds(ids)) {
        const cursor = coll
          .find({ ...matchQuery, "meta.device_id": id }, { projection: collectionName === ACCEL ? ACCEL_PROJECT_MIN : FREQ_PROJECT_MIN })
          .sort({ ts: -1 })
          .limit(Math.min(Number(limit || HISTORY_DEFAULT_N), HISTORY_DEFAULT_N))
          .hint(hint || { "meta.device_id": 1, ts: -1 })
          .maxTimeMS(QUERY_MAX_MS);

        const docsDesc = await cursor.toArray();
        map.set(String(id), docsDesc.reverse());
      }
      return map;
    }
  }
}

export async function historyByBridge(bridgeId, limit = HISTORY_DEFAULT_N) {
  // força limite máximo fixo (proteção)
  limit = Math.min(Number(limit || HISTORY_DEFAULT_N), HISTORY_DEFAULT_N);

  const variants = idVariants(bridgeId);

  const devs = await Device.find({ bridge_id: { $in: variants }, isActive: { $ne: false } })
    .select("device_id modo_operacao")
    .lean();

  if (devs.length === 0) {
    return { bridge_id: bridgeId, items: [] };
  }

  const deviceCodes = devs.map(d => String(d.device_id));

  const accelMap = await lastNPerDevice(
    ACCEL,
    { "meta.bridge_id": { $in: variants }, "meta.device_id": { $in: deviceCodes } },
    limit,
    deviceCodes
  );

  const freqMap  = await lastNPerDevice(
    FREQ,
    { "meta.bridge_id": { $in: variants }, "meta.device_id": { $in: deviceCodes } },
    limit,
    deviceCodes
  );

  const items = devs.map(d => {
    const id = String(d.device_id);
    const accel = accelMap.get(id) || [];
    const freq  = freqMap.get(id)  || [];
    return { device_id: id, accel, freq };
  });

  return { bridge_id: bridgeId, items };
}

/* =====================================================================
 * Índices recomendados
 * ===================================================================*/
export async function ensureTelemetryIndexes() {
  const db = mongoose.connection.db;

  await db.collection(ACCEL).createIndex({ "meta.bridge_id": 1,  "meta.device_id": 1, ts: -1 });
  await db.collection(ACCEL).createIndex({ "meta.company_id": 1, "meta.device_id": 1, ts: -1 });

  await db.collection(FREQ ).createIndex({ "meta.bridge_id": 1,  "meta.device_id": 1, ts: -1 });
  await db.collection(FREQ ).createIndex({ "meta.company_id": 1, "meta.device_id": 1, ts: -1 });

  // fallback simples
  await db.collection(ACCEL).createIndex({ "meta.device_id": 1, ts: -1 });
  await db.collection(FREQ ).createIndex({ "meta.device_id": 1, ts: -1 });
}
