// src/services/rollup/hourlyAccel.js
const BR_OFFSET_MS = 3 * 60 * 60 * 1000;

const RAW = "telemetry_ts_accel";
const OUT = "telemetry_rollup_hourly_accel";

function nowMs() { return Date.now(); }

/**
 * Rollup hourly ACCEL
 * - fromUtc/toUtc: Date em UTC
 * - salva:
 *   ts     (Date UTC do início da hora BR, mas armazenado como instante UTC)
 *   ts_br  (string "YYYY-MM-DDTHH:00:00.000" em horário BR, igual seu raw)
 *   bucket_br (igual ts, mantido para chave/índice)
 */
export async function rollupHourlyAccel(db, { fromUtc, toUtc }) {
  const t0 = nowMs();
  if (!(fromUtc instanceof Date) || !(toUtc instanceof Date)) {
    throw new Error("rollupHourlyAccel: fromUtc/toUtc devem ser Date");
  }

  const pipeline = [
    { $match: { ts: { $gte: fromUtc, $lt: toUtc } } },

    // ts -> ms
    { $addFields: { _tsMs: { $toLong: "$ts" } } },

    // ms no “relógio BR” (numérico)
    { $addFields: { _brMs: { $subtract: ["$_tsMs", BR_OFFSET_MS] } } },

    // início da hora no “relógio BR”
    { $addFields: { _hourBrMs: { $subtract: ["$_brMs", { $mod: ["$_brMs", 3600000] }] } } },

    // volta pra UTC (instante) para salvar como Date
    { $addFields: { bucket_br: { $toDate: { $add: ["$_hourBrMs", BR_OFFSET_MS] } } } },

    // string BR (sem timezone) igual seu raw
    {
      $addFields: {
        ts_br: {
          $dateToString: {
            date: { $toDate: "$_hourBrMs" }, // aqui é “data” no relógio BR (mas como ms)
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
          axis: "$meta.axis",
          bucket_br: "$bucket_br",
          ts_br: "$ts_br",
        },

        avg: { $avg: "$value" },
        min: { $min: "$value" },
        max: { $max: "$value" },
        count: { $sum: 1 },

        n_normal: { $sum: { $cond: [{ $eq: ["$severity", "normal"] }, 1, 0] } },
        n_alerta: { $sum: { $cond: [{ $eq: ["$severity", "alerta"] }, 1, 0] } },
        n_critico:{ $sum: { $cond: [{ $eq: ["$severity", "critico"] }, 1, 0] } },
      },
    },

    {
      $project: {
        _id: 0,
        company_id: "$_id.company_id",
        bridge_id: "$_id.bridge_id",
        device_id: "$_id.device_id",
        axis: "$_id.axis",

        ts: "$_id.bucket_br",       // Date UTC
        ts_br: "$_id.ts_br",        // string BR

        bucket_br: "$_id.bucket_br",

        avg: 1,
        min: 1,
        max: 1,
        count: 1,

        n_normal: 1,
        n_alerta: 1,
        n_critico: 1,
      },
    },

    {
      $merge: {
        into: OUT,
        on: ["company_id", "bridge_id", "device_id", "axis", "bucket_br"],
        whenMatched: "replace",
        whenNotMatched: "insert",
      },
    },
  ];

  await db.collection(RAW).aggregate(pipeline, { allowDiskUse: true }).toArray();
  return { ms: nowMs() - t0 };
}