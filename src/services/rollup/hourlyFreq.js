// src/services/rollup/hourlyFreq.js
const BR_OFFSET_MS = 3 * 60 * 60 * 1000;

const RAW = "telemetry_ts_freq_peaks";
const OUT = "telemetry_rollup_hourly_freq";

function nowMs() { return Date.now(); }

export async function rollupHourlyFreq(db, { fromUtc, toUtc }) {
  const t0 = nowMs();
  if (!(fromUtc instanceof Date) || !(toUtc instanceof Date)) {
    throw new Error("rollupHourlyFreq: fromUtc/toUtc devem ser Date");
  }

  const pipeline = [
    { $match: { ts: { $gte: fromUtc, $lt: toUtc } } },

    { $addFields: { _tsMs: { $toLong: "$ts" } } },
    { $addFields: { _brMs: { $subtract: ["$_tsMs", BR_OFFSET_MS] } } },
    { $addFields: { _hourBrMs: { $subtract: ["$_brMs", { $mod: ["$_brMs", 3600000] }] } } },
    { $addFields: { bucket_br: { $toDate: { $add: ["$_hourBrMs", BR_OFFSET_MS] } } } },

    // extrai picos do array peaks
    {
      $addFields: {
        _p0: { $arrayElemAt: ["$peaks", 0] },
        _p1: { $arrayElemAt: ["$peaks", 1] },
      },
    },
    {
      $addFields: {
        f1: "$_p0.f",
        mag1: "$_p0.mag",
        f2: "$_p1.f",
        mag2: "$_p1.mag",
      },
    },

    // string BR (sem timezone) igual seu raw
    {
      $addFields: {
        ts_br: {
          $dateToString: {
            date: { $toDate: "$_hourBrMs" },
            format: "%Y-%m-%dT%H:00:00.000",
            timezone: "UTC",
          },
        },
      },
    },

    {
      $group: {
        _id: {
          company_id: "$meta.company_id",
          bridge_id: "$meta.bridge_id",
          device_id: "$meta.device_id",
          stream: "$meta.stream",
          bucket_br: "$bucket_br",
          ts_br: "$ts_br",
        },

        f1_avg: { $avg: "$f1" },
        mag1_avg: { $avg: "$mag1" },
        f2_avg: { $avg: "$f2" },
        mag2_avg: { $avg: "$mag2" },

        count: { $sum: 1 },

        n_has_f1: { $sum: { $cond: [{ $ne: ["$f1", null] }, 1, 0] } },
        n_has_f2: { $sum: { $cond: [{ $ne: ["$f2", null] }, 1, 0] } },
        n_atividade: { $sum: { $cond: [{ $eq: ["$status", "atividade_detectada"] }, 1, 0] } },
      },
    },

    {
      $project: {
        _id: 0,
        company_id: "$_id.company_id",
        bridge_id: "$_id.bridge_id",
        device_id: "$_id.device_id",
        stream: "$_id.stream",

        ts: "$_id.bucket_br",    // Date UTC
        ts_br: "$_id.ts_br",     // string BR

        bucket_br: "$_id.bucket_br",

        f1_avg: 1,
        mag1_avg: 1,
        f2_avg: 1,
        mag2_avg: 1,

        count: 1,
        n_has_f1: 1,
        n_has_f2: 1,
        n_atividade: 1,
      },
    },

    {
      $merge: {
        into: OUT,
        on: ["company_id", "bridge_id", "device_id", "stream", "bucket_br"],
        whenMatched: "replace",
        whenNotMatched: "insert",
      },
    },
  ];

  await db.collection(RAW).aggregate(pipeline, { allowDiskUse: true }).toArray();
  return { ms: nowMs() - t0 };
}