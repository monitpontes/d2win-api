// src/services/export/exportDayD5.js
import { putObjectBuffer } from "../putObjectBuffer.js";
import { cursorToParquetBuffer } from "./mongoToParquet.js";

/**
 * EXPORT D-5 (BR) -> S3
 *
 * - Exporta 1 dia inteiro (BR) "D-5" para S3:
 *   RAW:  telemetry_ts_accel, telemetry_ts_freq_peaks
 *   HOURLY: telemetry_rollup_hourly_accel, telemetry_rollup_hourly_freq
 *   DAILY: telemetry_rollup_daily_accel, telemetry_rollup_daily_freq
 *
 * - Usa Mongo -> Parquet direto (sem JSON/DuckDB) via cursorToParquetBuffer()
 *
 * Observação:
 * - Janela do dia é BR (America/Sao_Paulo), mas em UTC no Mongo:
 *   startUtc = YYYY-MM-DDT00:00:00-03:00
 *   endUtc   = startUtc + 1 dia
 */

const DAY_MS = 86_400_000;
const TZ_BR = "America/Sao_Paulo";

// Coleções
const COLL = {
  raw_accel: "telemetry_ts_accel",
  raw_freq: "telemetry_ts_freq_peaks",
  hourly_accel: "telemetry_rollup_hourly_accel",
  hourly_freq: "telemetry_rollup_hourly_freq",
  daily_accel: "telemetry_rollup_daily_accel",
  daily_freq: "telemetry_rollup_daily_freq",
};

function brNow() {
  // Date "na zona BR" (truque via toLocaleString)
  return new Date(new Date().toLocaleString("en-US", { timeZone: TZ_BR }));
}

function getDayBr(offsetDays) {
  const d = brNow();
  d.setHours(0, 0, 0, 0);
  d.setDate(d.getDate() - offsetDays);

  const Y = d.getFullYear();
  const M = String(d.getMonth() + 1).padStart(2, "0");
  const D = String(d.getDate()).padStart(2, "0");
  return `${Y}-${M}-${D}`;
}

function dayWindowUtc(dayBr) {
  // início do dia BR em UTC (via offset fixo -03:00 como você está usando)
  const startUtc = new Date(`${dayBr}T00:00:00-03:00`);
  const endUtc = new Date(startUtc.getTime() + DAY_MS);
  return { startUtc, endUtc };
}

/**
 * Paths no S3 (1 arquivo por dia, dentro do mês):
 * telemetry_accel/raw/2026/03/accel_raw_2026-03-01.parquet
 * telemetry_accel/agg/2026/03/hourly/accel_hourly_2026-03-01.parquet
 * telemetry_accel/agg/2026/03/daily/accel_daily_2026-03-01.parquet
 */
function s3Key(domain, type, dayBr) {
  const [Y, M] = dayBr.split("-");
  if (type === "raw") {
    return `telemetry_${domain}/raw/${Y}/${M}/${domain}_raw_${dayBr}.parquet`;
  }
  return `telemetry_${domain}/agg/${Y}/${M}/${type}/${domain}_${type}_${dayBr}.parquet`;
}

async function hasData(db, coll, startUtc, endUtc) {
  // usa índice de ts e só checa existência
  const doc = await db
    .collection(coll)
    .findOne({ ts: { $gte: startUtc, $lt: endUtc } }, { projection: { _id: 1 } });
  return !!doc;
}

async function exportCollectionFast({ db, coll, query, s3key, batchSize = 50_000 }) {
  const cursor = db.collection(coll).find(query, { projection: { _id: 0 } });

  const out = await cursorToParquetBuffer(cursor, { batchSize });
  if (!out) return { ok: false, reason: "empty_cursor" };

  await putObjectBuffer({
    Key: s3key,
    Body: out.buffer,
    ContentType: "application/octet-stream",
  });

  return { ok: true, rows: out.rows };
}

/**
 * Exporta o dia D-5 (BR) para S3.
 * Se não houver nenhum dado em nenhuma collection naquele dia: exported=false.
 */
export async function exportD5ToS3(db, { offsetDays = 5 } = {}) {
  const dayBr = getDayBr(offsetDays);
  const { startUtc, endUtc } = dayWindowUtc(dayBr);

  // checagens rápidas (evita export à toa)
  const checks = await Promise.all([
    hasData(db, COLL.raw_accel, startUtc, endUtc),
    hasData(db, COLL.raw_freq, startUtc, endUtc),
    hasData(db, COLL.hourly_accel, startUtc, endUtc),
    hasData(db, COLL.hourly_freq, startUtc, endUtc),
    hasData(db, COLL.daily_accel, startUtc, endUtc),
    hasData(db, COLL.daily_freq, startUtc, endUtc),
  ]);

  if (!checks.some(Boolean)) {
    return {
      exported: false,
      reason: "no data for day",
      dayBr,
      window: { startUtc: startUtc.toISOString(), endUtc: endUtc.toISOString() },
    };
  }

  const results = {};

  if (checks[0]) {
    results.raw_accel = await exportCollectionFast({
      db,
      coll: COLL.raw_accel,
      query: { ts: { $gte: startUtc, $lt: endUtc } },
      s3key: s3Key("accel", "raw", dayBr),
    });
  }

  if (checks[1]) {
    results.raw_freq = await exportCollectionFast({
      db,
      coll: COLL.raw_freq,
      query: { ts: { $gte: startUtc, $lt: endUtc } },
      s3key: s3Key("freq", "raw", dayBr),
    });
  }

  // HOURLY e DAILY: também filtram por ts (Date)
  if (checks[2]) {
    results.hourly_accel = await exportCollectionFast({
      db,
      coll: COLL.hourly_accel,
      query: { ts: { $gte: startUtc, $lt: endUtc } },
      s3key: s3Key("accel", "hourly", dayBr),
    });
  }

  if (checks[3]) {
    results.hourly_freq = await exportCollectionFast({
      db,
      coll: COLL.hourly_freq,
      query: { ts: { $gte: startUtc, $lt: endUtc } },
      s3key: s3Key("freq", "hourly", dayBr),
    });
  }

  if (checks[4]) {
    results.daily_accel = await exportCollectionFast({
      db,
      coll: COLL.daily_accel,
      query: { ts: { $gte: startUtc, $lt: endUtc } },
      s3key: s3Key("accel", "daily", dayBr),
    });
  }

  if (checks[5]) {
    results.daily_freq = await exportCollectionFast({
      db,
      coll: COLL.daily_freq,
      query: { ts: { $gte: startUtc, $lt: endUtc } },
      s3key: s3Key("freq", "daily", dayBr),
    });
  }

  return {
    exported: true,
    dayBr,
    window: { startUtc: startUtc.toISOString(), endUtc: endUtc.toISOString() },
    results,
  };
}