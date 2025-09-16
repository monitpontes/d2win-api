// src/services/telemetry.js
import mongoose from "mongoose";
import Device from "../models/device.js";
import BridgeStatus from "../models/bridgeStatus.js";
import { toBrazilISOFromUTC, brazilPartsFromUTC } from "../lib/time.js";

/** Coleções cruas (time-series) usadas via driver nativo */
const ACCEL = "telemetry_ts_accel";
const FREQ  = "telemetry_ts_freq_peaks";
const STAT  = "telemetry_ts_device_status";

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

/** Helper opcional: atualiza last_seen do device ao receber telemetria */
export async function touchDeviceLastSeen(device_id, infos = undefined) {
  const set = { last_seen: new Date() };
  if (infos && typeof infos === "object") set.infos = infos;
  await Device.updateOne({ device_id }, { $set: set });
}

/* =====================================================================
 * SNAPSHOT POR PONTE (UM DOCUMENTO POR bridge_id)
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
                              .select("device_id last_seen infos");

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

/**
 * Varre todas as pontes existentes (agrupando devices)
 * e atualiza os snapshots.
 */
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
