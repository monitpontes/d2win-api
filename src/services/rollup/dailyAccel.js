// src/services/rollup/dailyAccel.js
const BR_OFFSET_MS = 3 * 60 * 60 * 1000;

const HOURLY_COLL = "telemetry_rollup_hourly_accel";
const DAILY_COLL = "telemetry_rollup_daily_accel";

function nowMs() {
  return Date.now();
}

export async function rollupDailyAccel(db, { fromUtc, toUtc }) {
  const t0 = nowMs();
  if (!(fromUtc instanceof Date) || !(toUtc instanceof Date)) {
    throw new Error("rollupDailyAccel: fromUtc/toUtc devem ser Date");
  }

  const hourly = db.collection(HOURLY_COLL);

  // Suporta hourly com campos:
  // - avg/min/max/count (seu padrão atual)
  // - e opcionalmente std (ou value_std) caso você tenha
  const pipeline = [
    { $match: { bucket_br: { $gte: fromUtc, $lt: toUtc } } },

    // bucket_br é Date -> converte pra ms numérico para poder usar $mod
    { $addFields: { _bucketMs: { $toLong: "$bucket_br" } } },
    { $addFields: { _brMs: { $subtract: ["$_bucketMs", BR_OFFSET_MS] } } },
    {
      $addFields: {
        _dayBrMs: { $subtract: ["$_brMs", { $mod: ["$_brMs", 86400000] }] },
      },
    },
    { $addFields: { _dayUtcMs: { $add: ["$_dayBrMs", BR_OFFSET_MS] } } },
    { $addFields: { bucket_day_utc: { $toDate: "$_dayUtcMs" } } },

    // Normaliza nomes (caso você tenha value_avg/value_std no hourly)
    {
      $addFields: {
        _avg: { $ifNull: ["$avg", "$value_avg"] },
        _min: { $ifNull: ["$min", "$value_min"] },
        _max: { $ifNull: ["$max", "$value_max"] },
        _count: { $ifNull: ["$count", "$n_points"] },
        _std: { $ifNull: ["$std", "$value_std"] }, // pode ser null
        _n_normal: { $ifNull: ["$n_normal", 0] },
        _n_alerta: { $ifNull: ["$n_alerta", 0] },
        _n_critico: { $ifNull: ["$n_critico", 0] },
      },
    },

    {
      $group: {
        _id: {
          company_id: "$company_id",
          bridge_id: "$bridge_id",
          device_id: "$device_id",
          axis: "$axis",
          bucket_br: "$bucket_day_utc",
        },

        count: { $sum: "$_count" },

        // soma ponderada p/ média diária
        _sum: { $sum: { $multiply: ["$_avg", "$_count"] } },

        // std diário aproximado (se hourly tiver std)
        // sumsq = (std^2 + avg^2) * n
        _sumsq: {
          $sum: {
            $multiply: [
              {
                $add: [
                  { $multiply: [{ $ifNull: ["$_std", 0] }, { $ifNull: ["$_std", 0] }] },
                  { $multiply: ["$_avg", "$_avg"] },
                ],
              },
              "$_count",
            ],
          },
        },

        min: { $min: "$_min" },
        max: { $max: "$_max" },

        n_normal: { $sum: "$_n_normal" },
        n_alerta: { $sum: "$_n_alerta" },
        n_critico: { $sum: "$_n_critico" },
      },
    },

    {
      $addFields: {
        company_id: "$_id.company_id",
        bridge_id: "$_id.bridge_id",
        device_id: "$_id.device_id",
        axis: "$_id.axis",
        bucket_br: "$_id.bucket_br",

        avg: { $cond: [{ $gt: ["$count", 0] }, { $divide: ["$_sum", "$count"] }, null] },
      },
    },

    // std diário (se hourly tiver std; se não tiver, vira uma aproximação “sem variação intra-hora”)
    {
      $addFields: {
        std: {
          $let: {
            vars: { n: "$count", mean: "$avg", sumsq: "$_sumsq" },
            in: {
              $cond: [
                { $gt: ["$$n", 0] },
                {
                  $sqrt: {
                    $max: [
                      0,
                      { $subtract: [{ $divide: ["$$sumsq", "$$n"] }, { $multiply: ["$$mean", "$$mean"] }] },
                    ],
                  },
                },
                null,
              ],
            },
          },
        },
      },
    },

    // ✅ ts (UTC) e ts_br (BR)
    {
      $addFields: {
        ts: "$bucket_br",
        ts_br: {
          $dateSubtract: {
            startDate: "$bucket_br",
            unit: "hour",
            amount: 3,
          },
        },
      },
    },

    {
      $project: {
        _id: 0,
        company_id: 1,
        bridge_id: 1,
        device_id: 1,
        axis: 1,

        ts: 1,
        ts_br: 1,

        bucket_br: 1,

        avg: 1,
        min: 1,
        max: 1,
        std: 1,
        count: 1,

        n_normal: 1,
        n_alerta: 1,
        n_critico: 1,
      },
    },

    {
      $merge: {
        into: DAILY_COLL,
        on: ["company_id", "bridge_id", "device_id", "axis", "bucket_br"],
        whenMatched: "replace",
        whenNotMatched: "insert",
      },
    },
  ];

  await hourly.aggregate(pipeline, { allowDiskUse: true }).toArray();
  return { ms: nowMs() - t0 };
}