// src/services/telemetry.js
import mongoose from "mongoose";
import Device from "../models/device.js";
import BridgeStatus from "../models/bridgeStatus.js";
import { toBrazilISOFromUTC, brazilPartsFromUTC } from "../lib/time.js";

/** Coleções cruas (time-series) usadas via driver nativo */
const ACCEL = "telemetry_ts_accel";
const FREQ  = "telemetry_ts_freq_peaks";
const STAT  = "telemetry_ts_device_status"; // opcional

/* =====================================================================
 * 🔧 Configs de proteção/limites (ADICIONADO)
 * ===================================================================*/
const HISTORY_DEFAULT_N = Number(process.env.HISTORY_DEFAULT_N || 10);  // últimos 10 p/ history
const DEVICE_HARD_CAP   = Number(process.env.DEVICE_HARD_CAP   || 2000);
const QUERY_MAX_MS      = Number(process.env.QUERY_MAX_MS      || 3000);

// projeções mínimas p/ reduzir bytes processados
const ACCEL_PROJECT_MIN = {
  _id: 0,
  ts: 1,
  value: 1, rms: 1, ax: 1, ay: 1, az: 1, severity: 1,
  "meta.device_id": 1,
  "meta.severity": 1,
  // campos extras comuns:
  fw: 1, units: 1
};

const FREQ_PROJECT_MIN = {
  _id: 0,
  ts: 1,
  status: 1, fs: 1, n: 1,
  peak: 1, dom_freq: 1, peaks: 1, severity: 1,
  "metrics.peak": 1, "metrics.dom_freq": 1, "metrics.peaks": 1,
  "meta.device_id": 1,
  "meta.severity": 1,
  fw: 1
};

/* =====================================================================
 * INSERÇÕES DE TELEMETRIA
 * ===================================================================*/

// Aceleração
export async function insertAccel({
  company_id, bridge_id, device_id,
  ts, axis = "z", value, fw,
  severity = null,
}) {
  const tsUTC = ts ? new Date(ts) : new Date();
  const { ts_br, date_br, hour_br } = brazilPartsFromUTC(tsUTC);

  const doc = {
    ts: tsUTC,
    ts_br, date_br, hour_br,
    meta: { company_id, bridge_id, device_id, stream: `accel:${axis}`, severity },
    value, units: "m/s2", fw,
    severity,
  };
  return mongoose.connection.db.collection(ACCEL).insertOne(doc);
}

// Frequência (picos/keep-alive)
export async function insertFreqPeaks({
  company_id, bridge_id, device_id,
  ts, status, fs, n, peaks, fw,
  severity = null,
}) {
  const tsUTC = ts ? new Date(ts) : new Date();
  const { ts_br, date_br, hour_br } = brazilPartsFromUTC(tsUTC);

  const doc = {
    ts: tsUTC,
    ts_br, date_br, hour_br,
    meta: { company_id, bridge_id, device_id, stream: "freq:z", severity },
    status, fs, n, peaks, fw,
    severity,
  };
  return mongoose.connection.db.collection(FREQ).insertOne(doc);
}

// Status do dispositivo (histórico opcional)
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
 * LATEST (p/ Dashboard / Bridge Page)
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

// ⚠️ ADICIONADO: hard cap de devices por requisição
function clampDeviceIds(deviceIds = []) {
  if (!Array.isArray(deviceIds)) return [];
  if (deviceIds.length > DEVICE_HARD_CAP) {
    return deviceIds.slice(0, DEVICE_HARD_CAP);
  }
  return deviceIds;
}

async function latestPerDevice(collectionName, matchQuery) {
  const coll = mongoose.connection.db.collection(collectionName);

  const byCompany = Object.prototype.hasOwnProperty.call(matchQuery, "meta.company_id");
  const byBridge  = Object.prototype.hasOwnProperty.call(matchQuery, "meta.bridge_id");

  const sortStage = byCompany
    ? { "meta.company_id": 1, "meta.device_id": 1, ts: -1 }
    : { "meta.bridge_id": 1,  "meta.device_id": 1, ts: -1 };

  const hint = getHintFor(matchQuery);

  // 🔎 Projeção mínima + limit de segurança (ADICIONADO)
  const projectMin = collectionName === ACCEL ? ACCEL_PROJECT_MIN : FREQ_PROJECT_MIN;

  const pipeline = [
    { $match: matchQuery },
    { $sort: sortStage },
    { $group: { _id: "$meta.device_id", doc: { $first: "$$ROOT" } } },
    { $project: { _id: 0, doc: projectMin } },
    { $limit: DEVICE_HARD_CAP } // segurança contra grupos excessivos
  ];

  try {
    const cursor = coll.aggregate(pipeline, { allowDiskUse: false, hint, maxTimeMS: QUERY_MAX_MS });
    const rows = await cursor.toArray();
    const map = new Map();
    for (const r of rows) {
      const dev = r.doc?.meta?.device_id ?? r._id;
      map.set(String(dev), r.doc);
    }
    return map;
  } catch {
    const rows = await coll.aggregate(pipeline, { allowDiskUse: false }).toArray();
    const map = new Map();
    for (const r of rows) {
      const dev = r.doc?.meta?.device_id ?? r._id;
      map.set(String(dev), r.doc);
    }
    return map;
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
      accel: accel ? {
        ts: accel.ts,
        value: accel.value ?? null,
        rms:   accel.rms   ?? accel?.metrics?.rms ?? null,
        ax:    accel.ax ?? null,
        ay:    accel.ay ?? null,
        az:    accel.az ?? null,
        severity: accel.severity ?? accel?.meta?.severity ?? null,
      } : null,
      freq: freq ? {
        ts: freq.ts,
        status: freq.status ?? null,
        fs: freq.fs ?? null,
        n:  freq.n ?? null,
        peak:     freq.peak     ?? freq?.metrics?.peak ?? null,
        dom_freq: freq.dom_freq ?? freq?.metrics?.dom_freq ?? null,
        peaks:    freq.peaks    ?? freq?.metrics?.peaks ?? null,
        severity: freq.severity ?? freq?.meta?.severity ?? null,
      } : null,
    };
  });

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
      accel: accel ? {
        ts: accel.ts,
        value: accel.value ?? null,
        rms:   accel.rms   ?? accel?.metrics?.rms ?? null,
        ax:    accel.ax ?? null,
        ay:    accel.ay ?? null,
        az:    accel.az ?? null,
        severity: accel.severity ?? accel?.meta?.severity ?? null,
      } : null,
      freq: freq ? {
        ts: freq.ts,
        status: freq.status ?? null,
        fs: freq.fs ?? null,
        n:  freq.n ?? null,
        peak:     freq.peak     ?? freq?.metrics?.peak ?? null,
        dom_freq: freq.dom_freq ?? freq?.metrics?.dom_freq ?? null,
        peaks:    freq.peaks    ?? freq?.metrics?.peaks ?? null,
        severity: freq.severity ?? freq?.meta?.severity ?? null,
      } : null,
    };
  });

  return { bridge_id: bridgeId, company_ids: companyIds, updated_at: new Date(), devices: items };
}

/* =====================================================================
 * HISTORY por ponte (N últimos por device/stream)
 * ===================================================================*/

// Pega os N mais recentes por device, tentando pipelines modernos e
// caindo para consultas por device se não houver suporte no cluster.
async function lastNPerDevice(collectionName, matchQuery, limit = 10, deviceIds = []) {
  const coll = mongoose.connection.db.collection(collectionName);

  // ⚠️ cap de devices (ADICIONADO)
  if (Array.isArray(deviceIds)) {
    deviceIds = clampDeviceIds(deviceIds);
  }

  const hint = getHintFor(matchQuery);

  // 1) Tenta $topN (MongoDB >= 5.2)
  const pipelineTopN = [
    { $match: matchQuery },
    {
      $group: {
        _id: "$meta.device_id",
        docs: { $topN: { sortBy: { ts: -1 }, output: "$$ROOT", n: Math.min(Number(limit || HISTORY_DEFAULT_N), HISTORY_DEFAULT_N) } }
      }
    },
    // devolver em ordem cronológica crescente
    { $project: { _id: 0, device_id: "$_id", docs: { $reverseArray: "$docs" } } },
    { $limit: DEVICE_HARD_CAP } // segurança
  ];

  try {
    const arr = await coll.aggregate(pipelineTopN, {
      allowDiskUse: false,
      hint,
      maxTimeMS: QUERY_MAX_MS
    }).toArray();

    const map = new Map();
    for (const r of arr) {
      // projeção mínima por documento (aplicada após seleção)
      r.docs = r.docs.map(d => {
        if (collectionName === ACCEL) {
          const { ts, value, rms, ax, ay, az, severity, meta, fw, units } = d;
          return { ts, value, rms, ax, ay, az, severity, meta: { device_id: meta?.device_id, severity: meta?.severity }, fw, units };
        } else {
          const { ts, status, fs, n, peak, dom_freq, peaks, severity, metrics, meta, fw } = d;
          return {
            ts, status, fs, n,
            peak: peak ?? metrics?.peak,
            dom_freq: dom_freq ?? metrics?.dom_freq,
            peaks: peaks ?? metrics?.peaks,
            severity,
            meta: { device_id: meta?.device_id, severity: meta?.severity },
            fw
          };
        }
      });
      map.set(String(r.device_id), r.docs);
    }
    return map;
  } catch (errTopN) {
    // 2) Fallback: janela (MongoDB >= 5.0)
    const pipelineWindow = [
      { $match: matchQuery },
      {
        $setWindowFields: {
          partitionBy: "$meta.device_id",
          sortBy: { ts: -1 },
          output: { rk: { $documentNumber: {} } }
        }
      },
      { $match: { rk: { $lte: Math.min(Number(limit || HISTORY_DEFAULT_N), HISTORY_DEFAULT_N) } } },
      { $sort: { "meta.device_id": 1, ts: 1 } }, // voltar cronológico
      { $group: { _id: "$meta.device_id", docs: { $push: "$$ROOT" } } },
      { $project: { _id: 0, device_id: "$_id", docs: 1 } },
      { $limit: DEVICE_HARD_CAP }
    ];

    try {
      const arr = await coll.aggregate(pipelineWindow, {
        allowDiskUse: false,
        hint,
        maxTimeMS: QUERY_MAX_MS
      }).toArray();

      const map = new Map();
      for (const r of arr) {
        r.docs = r.docs.map(d => {
          if (collectionName === ACCEL) {
            const { ts, value, rms, ax, ay, az, severity, meta, fw, units } = d;
            return { ts, value, rms, ax, ay, az, severity, meta: { device_id: meta?.device_id, severity: meta?.severity }, fw, units };
          } else {
            const { ts, status, fs, n, peak, dom_freq, peaks, severity, metrics, meta, fw } = d;
            return {
              ts, status, fs, n,
              peak: peak ?? metrics?.peak,
              dom_freq: dom_freq ?? metrics?.dom_freq,
              peaks: peaks ?? metrics?.peaks,
              severity,
              meta: { device_id: meta?.device_id, severity: meta?.severity },
              fw
            };
          }
        });
        map.set(String(r.device_id), r.docs);
      }
      return map;
    } catch (errWindow) {
      // 3) Fallback final: 1 find por device (compatível com qualquer versão)
      const ids = deviceIds.length
        ? deviceIds
        : await coll.distinct("meta.device_id", matchQuery);

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
  // força limite máximo = HISTORY_DEFAULT_N (últimos 10) conforme pedido
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
 * (ADICIONADO) Garante índices que casam com as consultas
 * ===================================================================*/
export async function ensureTelemetryIndexes() {
  const db = mongoose.connection.db;

  // Índices para latest por ponte e por empresa (sort por ts desc no sufixo)
  await db.collection(ACCEL).createIndex({ "meta.bridge_id": 1,  "meta.device_id": 1, ts: -1 });
  await db.collection(ACCEL).createIndex({ "meta.company_id": 1, "meta.device_id": 1, ts: -1 });

  await db.collection(FREQ ).createIndex({ "meta.bridge_id": 1,  "meta.device_id": 1, ts: -1 });
  await db.collection(FREQ ).createIndex({ "meta.company_id": 1, "meta.device_id": 1, ts: -1 });

  // (Opcional) índices simples por device+ts para fallback
  await db.collection(ACCEL).createIndex({ "meta.device_id": 1, ts: -1 });
  await db.collection(FREQ ).createIndex({ "meta.device_id": 1, ts: -1 });
}
