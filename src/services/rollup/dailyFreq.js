// src/services/rollup/dailyFreq.js
const BR_OFFSET_MS = 3 * 60 * 60 * 1000;

const HOURLY_COLL = "telemetry_rollup_hourly_freq";
const DAILY_COLL = "telemetry_rollup_daily_freq";

function nowMs() {
  return Date.now();
}

export async function rollupDailyFreq(db, { fromUtc, toUtc }) {
  const t0 = nowMs();
  if (!(fromUtc instanceof Date) || !(toUtc instanceof Date)) {
    throw new Error("rollupDailyFreq: fromUtc/toUtc devem ser Date");
  }

  const hourly = db.collection(HOURLY_COLL);

  const pipeline = [
    { $match: { bucket_br: { $gte: fromUtc, $lt: toUtc } } },

    { $addFields: { _bucketMs: { $toLong: "$bucket_br" } } },
    { $addFields: { _brMs: { $subtract: ["$_bucketMs", BR_OFFSET_MS] } } },
    {
      $addFields: {
        _dayBrMs: { $subtract: ["$_brMs", { $mod: ["$_brMs", 86400000] }] },
      },
    },
    { $addFields: { _dayUtcMs: { $add: ["$_dayBrMs", BR_OFFSET_MS] } } },
    { $addFields: { bucket_day_utc: { $toDate: "$_dayUtcMs" } } },

    {
      $addFields: {
        _count: { $ifNull: ["$count", 0] },
      },
    },

    {
      $group: {
        _id: {
          company_id: "$company_id",
          bridge_id: "$bridge_id",
          device_id: "$device_id",
          stream: "$stream",
          bucket_br: "$bucket_day_utc",
        },

        count: { $sum: "$_count" },

        _sum_f1: { $sum: { $multiply: ["$f1_avg", "$_count"] } },
        _sum_f2: { $sum: { $multiply: ["$f2_avg", "$_count"] } },
        _sum_mag1: { $sum: { $multiply: ["$mag1_avg", "$_count"] } },
        _sum_mag2: { $sum: { $multiply: ["$mag2_avg", "$_count"] } },

        // se existirem esses campos no hourly, soma (se não existir, fica 0)
        n_has_f1: { $sum: { $ifNull: ["$n_has_f1", 0] } },
        n_has_f2: { $sum: { $ifNull: ["$n_has_f2", 0] } },
        n_atividade: { $sum: { $ifNull: ["$n_atividade", 0] } },
        n_normal: { $sum: { $ifNull: ["$n_normal", 0] } },
        n_alerta: { $sum: { $ifNull: ["$n_alerta", 0] } },
        n_critico: { $sum: { $ifNull: ["$n_critico", 0] } },

        // se existir, pega max
        peak_f_max: { $max: { $ifNull: ["$peak_f_max", null] } },
        peak_mag_of_max_f: { $max: { $ifNull: ["$peak_mag_of_max_f", null] } },
      },
    },

    {
      $addFields: {
        company_id: "$_id.company_id",
        bridge_id: "$_id.bridge_id",
        device_id: "$_id.device_id",
        stream: "$_id.stream",
        bucket_br: "$_id.bucket_br",

        f1_avg: { $cond: [{ $gt: ["$count", 0] }, { $divide: ["$_sum_f1", "$count"] }, null] },
        f2_avg: { $cond: [{ $gt: ["$count", 0] }, { $divide: ["$_sum_f2", "$count"] }, null] },
        mag1_avg: { $cond: [{ $gt: ["$count", 0] }, { $divide: ["$_sum_mag1", "$count"] }, null] },
        mag2_avg: { $cond: [{ $gt: ["$count", 0] }, { $divide: ["$_sum_mag2", "$count"] }, null] },
      },
    },

    {
      $addFields: {
        ts: "$bucket_br",
        ts_br: {
          $dateSubtract: { startDate: "$bucket_br", unit: "hour", amount: 3 },
        },
      },
    },

    {
      $project: {
        _id: 0,
        company_id: 1,
        bridge_id: 1,
        device_id: 1,
        stream: 1,

        ts: 1,
        ts_br: 1,

        bucket_br: 1,

        f1_avg: 1,
        f2_avg: 1,
        mag1_avg: 1,
        mag2_avg: 1,
        count: 1,

        n_has_f1: 1,
        n_has_f2: 1,
        n_atividade: 1,
        n_normal: 1,
        n_alerta: 1,
        n_critico: 1,

        peak_f_max: 1,
        peak_mag_of_max_f: 1,
      },
    },

    {
      $merge: {
        into: DAILY_COLL,
        on: ["company_id", "bridge_id", "device_id", "stream", "bucket_br"],
        whenMatched: "replace",
        whenNotMatched: "insert",
      },
    },
  ];

  await hourly.aggregate(pipeline, { allowDiskUse: true }).toArray();
  return { ms: nowMs() - t0 };
}