// src/controllers/s3Telemetry.js
import {
  mustOneOf,
  safeToken,
  parseISODate,
  toDuckTsBR,
  RAW_TIME_COL,
  AGG_TIME_COL,
  keysForAggRange,
  keysForRawRange,
  buildWhere,
  buildWhereWithTime,
  duckdbQueryJsonFromS3Keys,
} from "../lib/s3Telemetry.js";

/**
 * COLD AGG (range) - accel
 * GET /telemetry/accel/agg?granularity=daily&from=...&to=...&device_id=...&axis=...
 */
export async function accelAggRange(req, res) {
  try {
    const granularity = mustOneOf(req.query.granularity, ["hourly", "daily", "monthly"], "granularity");
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
      device_id: safeToken(req.query.device_id, "device_id"),
      axis: safeToken(req.query.axis, "axis"),
    };

    const keys = keysForAggRange({ domain: "accel", granularity, from, to });
    const whereSql = buildWhere(filters);

    // AGG já está em BR (bucket_br). Vamos filtrar também em BR.
    const fromTs = toDuckTsBR(from);
    const toTs = toDuckTsBR(to);

    const timeClause =
      (whereSql ? " AND " : "WHERE ") +
      `${AGG_TIME_COL} >= TIMESTAMP '${fromTs}' AND ${AGG_TIME_COL} < TIMESTAMP '${toTs}'`;

    const sqlSelect = `
SELECT
  company_id,
  bridge_id,
  device_id,
  axis,
  ${AGG_TIME_COL} AS bucket_br,
  value_avg,
  value_min,
  value_max,
  value_std,
  n_points,
  n_normal,
  n_alerta,
  n_critico
FROM v
${whereSql}${timeClause}
ORDER BY ${AGG_TIME_COL}, device_id, axis
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-agg-range",
      sqlSelect,
    });

    return res.json({
      domain: "accel",
      source: "s3_agg",
      granularity,
      from: from.toISOString(),
      to: to.toISOString(),
      s3_keys: keys,
      count: rows.length,
      rows,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

/**
 * COLD AGG (range) - freq
 * GET /telemetry/freq/agg?granularity=daily&from=...&to=...&device_id=...&stream=...
 */
export async function freqAggRange(req, res) {
  try {
    const granularity = mustOneOf(req.query.granularity, ["hourly", "daily", "monthly"], "granularity");
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
      device_id: safeToken(req.query.device_id, "device_id"),
      stream: safeToken(req.query.stream, "stream"),
    };

    const keys = keysForAggRange({ domain: "freq", granularity, from, to });
    const whereSql = buildWhere(filters);

    const fromTs = toDuckTsBR(from);
    const toTs = toDuckTsBR(to);

    const timeClause =
      (whereSql ? " AND " : "WHERE ") +
      `${AGG_TIME_COL} >= TIMESTAMP '${fromTs}' AND ${AGG_TIME_COL} < TIMESTAMP '${toTs}'`;

    const sqlSelect = `
SELECT
  company_id,
  bridge_id,
  device_id,
  stream,
  ${AGG_TIME_COL} AS bucket_br,
  f1_avg,
  mag1_avg,
  f2_avg,
  mag2_avg,
  n_points,
  n_has_f1,
  n_has_f2,
  n_atividade,
  peak_f_max,
  peak_mag_of_max_f,
  peak_tag_of_max_f
FROM v
${whereSql}${timeClause}
ORDER BY ${AGG_TIME_COL}, device_id, stream
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-agg-range",
      sqlSelect,
    });

    return res.json({
      domain: "freq",
      source: "s3_agg",
      granularity,
      from: from.toISOString(),
      to: to.toISOString(),
      s3_keys: keys,
      count: rows.length,
      rows,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

/**
 * COLD RAW (range) - accel
 * GET /telemetry/accel/raw?from=...&to=...&device_id=...&axis=z&limit=20000&order=asc
 *
 * ⚠️ Para segurança/custo: device_id obrigatório.
 */
export async function accelRawRange(req, res) {
  try {
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const device_id = safeToken(req.query.device_id, "device_id");
    if (!device_id) return res.status(400).json({ error: "device_id é obrigatório no RAW" });

    const axis = safeToken(req.query.axis, "axis");
    const limit = Math.min(Number(req.query.limit || 20000), 100000);
    const order = mustOneOf((req.query.order || "asc").toLowerCase(), ["asc", "desc"], "order");

    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
      device_id,
      axis,
    };

    const keys = keysForRawRange({ domain: "accel", from, to });

    // Filtrar pelo tempo BR (ts_br_ts) => converte from/to para BR antes
    const fromTs = toDuckTsBR(from);
    const toTs = toDuckTsBR(to);

    const whereSql = buildWhereWithTime({
      filters,
      timeCol: RAW_TIME_COL, // ts_br_ts
      fromTs,
      toTs,
    });

    // RAW accel: ts_br_ts, value (e axis)
    const sqlSelect = `
SELECT
  company_id,
  bridge_id,
  device_id,
  axis,
  ${RAW_TIME_COL} AS ts_br_ts,
  value
FROM v
${whereSql}
ORDER BY ${RAW_TIME_COL} ${order}
LIMIT ${limit}
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-raw-range",
      sqlSelect,
    });

    return res.json({
      domain: "accel",
      source: "s3_raw",
      from: from.toISOString(),
      to: to.toISOString(),
      time_col: RAW_TIME_COL,
      s3_keys: keys,
      count: rows.length,
      rows,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

/**
 * COLD RAW (range) - freq
 * GET /telemetry/freq/raw?from=...&to=...&device_id=...&stream=...&limit=20000
 */
export async function freqRawRange(req, res) {
  try {
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const device_id = safeToken(req.query.device_id, "device_id");
    if (!device_id) return res.status(400).json({ error: "device_id é obrigatório no RAW" });

    const stream = safeToken(req.query.stream, "stream");
    const limit = Math.min(Number(req.query.limit || 20000), 100000);
    const order = mustOneOf((req.query.order || "asc").toLowerCase(), ["asc", "desc"], "order");

    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
      device_id,
      stream,
    };

    const keys = keysForRawRange({ domain: "freq", from, to });

    const fromTs = toDuckTsBR(from);
    const toTs = toDuckTsBR(to);

    const whereSql = buildWhereWithTime({
      filters,
      timeCol: RAW_TIME_COL, // ts_br_ts
      fromTs,
      toTs,
    });

    const sqlSelect = `
SELECT
  company_id,
  bridge_id,
  device_id,
  stream,
  ${RAW_TIME_COL} AS ts_br_ts,
  f1,
  mag1,
  f2,
  mag2
FROM v
${whereSql}
ORDER BY ${RAW_TIME_COL} ${order}
LIMIT ${limit}
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-raw-range",
      sqlSelect,
    });

    return res.json({
      domain: "freq",
      source: "s3_raw",
      from: from.toISOString(),
      to: to.toISOString(),
      time_col: RAW_TIME_COL,
      s3_keys: keys,
      count: rows.length,
      rows,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

// ---- BOX PLOT (COLD S3 RAW) ----

// /telemetry/accel/boxplot?from=...&to=...&axis=z
export async function accelBoxplot(req, res) {
  try {
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const axis = safeToken(req.query.axis, "axis");
    const device_id = safeToken(req.query.device_id, "device_id"); // opcional
    const keys = keysForRawRange({ domain: "accel", from, to });

    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
      device_id,
      axis,
    };

    const whereSql = buildWhereWithTime({
      filters,
      timeCol: RAW_TIME_COL,
      fromTs: toDuckTsBR(from),
      toTs: toDuckTsBR(to),
    });

    const sqlSelect = `
SELECT
  COUNT(*)::BIGINT AS n,
  MIN(value)       AS min,
  MAX(value)       AS max,
  AVG(value)       AS mean,
  STDDEV_SAMP(value) AS std,
  approx_quantile(value, 0.25) AS q1,
  approx_quantile(value, 0.50) AS q2,
  approx_quantile(value, 0.75) AS q3,
  approx_quantile(value, 0.95) AS p95,
  approx_quantile(value, 0.99) AS p99
FROM v
${whereSql}
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-boxplot-accel",
      sqlSelect,
    });

    return res.json({
      domain: "accel",
      source: "s3_raw",
      time_col: RAW_TIME_COL,
      from: from.toISOString(),
      to: to.toISOString(),
      filters,
      s3_keys: keys,
      result: rows?.[0] || null,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

// /telemetry/freq/boxplot?metric=f1|mag1|f2|mag2&from=...&to=...
export async function freqBoxplot(req, res) {
  try {
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const metric = mustOneOf((req.query.metric || "f1").toLowerCase(), ["f1", "mag1", "f2", "mag2"], "metric");
    const stream = safeToken(req.query.stream, "stream");
    const device_id = safeToken(req.query.device_id, "device_id"); // opcional

    const keys = keysForRawRange({ domain: "freq", from, to });

    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
      device_id,
      stream,
    };

    const whereSql = buildWhereWithTime({
      filters,
      timeCol: RAW_TIME_COL,
      fromTs: toDuckTsBR(from),
      toTs: toDuckTsBR(to),
    });

    const sqlSelect = `
SELECT
  COUNT(*)::BIGINT AS n,
  MIN(${metric})   AS min,
  MAX(${metric})   AS max,
  AVG(${metric})   AS mean,
  STDDEV_SAMP(${metric}) AS std,
  approx_quantile(${metric}, 0.25) AS q1,
  approx_quantile(${metric}, 0.50) AS q2,
  approx_quantile(${metric}, 0.75) AS q3,
  approx_quantile(${metric}, 0.95) AS p95,
  approx_quantile(${metric}, 0.99) AS p99
FROM v
${whereSql}
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-boxplot-freq",
      sqlSelect,
    });

    return res.json({
      domain: "freq",
      source: "s3_raw",
      time_col: RAW_TIME_COL,
      metric,
      from: from.toISOString(),
      to: to.toISOString(),
      filters,
      s3_keys: keys,
      result: rows?.[0] || null,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

// ---- HISTOGRAMA (COLD S3 RAW) ----

// /telemetry/accel/hist?from=...&to=...&axis=z&bins=60
export async function accelHist(req, res) {
  try {
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const bins = Math.min(Math.max(Number(req.query.bins || 60), 10), 400);
    const axis = safeToken(req.query.axis, "axis");
    const device_id = safeToken(req.query.device_id, "device_id"); // opcional

    const keys = keysForRawRange({ domain: "accel", from, to });
    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
      device_id,
      axis,
    };

    const whereSql = buildWhereWithTime({
      filters,
      timeCol: RAW_TIME_COL,
      fromTs: toDuckTsBR(from),
      toTs: toDuckTsBR(to),
    });

    const sqlSelect = `
WITH base AS (
  SELECT value
  FROM v
  ${whereSql}
),
mm AS (
  SELECT MIN(value) AS vmin, MAX(value) AS vmax, COUNT(*)::BIGINT AS n
  FROM base
),
binned AS (
  SELECT
    mm.vmin,
    mm.vmax,
    mm.n,
    CASE
      WHEN mm.vmax = mm.vmin THEN 0
      ELSE CAST(FLOOR( (value - mm.vmin) / ((mm.vmax - mm.vmin) / ${bins}) ) AS BIGINT)
    END AS bin
  FROM base, mm
)
SELECT
  vmin,
  vmax,
  n,
  CASE WHEN bin < 0 THEN 0 WHEN bin >= ${bins} THEN ${bins}-1 ELSE bin END AS bin,
  COUNT(*)::BIGINT AS count
FROM binned
GROUP BY vmin, vmax, n, bin
ORDER BY bin;
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-hist-accel",
      sqlSelect,
    });

    return res.json({
      domain: "accel",
      source: "s3_raw",
      time_col: RAW_TIME_COL,
      bins,
      from: from.toISOString(),
      to: to.toISOString(),
      filters,
      s3_keys: keys,
      rows,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

// /telemetry/freq/hist?metric=f1|mag1|f2|mag2&from=...&to=...&bins=60
export async function freqHist(req, res) {
  try {
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const bins = Math.min(Math.max(Number(req.query.bins || 60), 10), 400);
    const metric = mustOneOf((req.query.metric || "f1").toLowerCase(), ["f1", "mag1", "f2", "mag2"], "metric");
    const stream = safeToken(req.query.stream, "stream");
    const device_id = safeToken(req.query.device_id, "device_id"); // opcional

    const keys = keysForRawRange({ domain: "freq", from, to });
    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
      device_id,
      stream,
    };

    const whereSql = buildWhereWithTime({
      filters,
      timeCol: RAW_TIME_COL,
      fromTs: toDuckTsBR(from),
      toTs: toDuckTsBR(to),
    });

    const sqlSelect = `
WITH base AS (
  SELECT ${metric} AS value
  FROM v
  ${whereSql}
),
mm AS (
  SELECT MIN(value) AS vmin, MAX(value) AS vmax, COUNT(*)::BIGINT AS n
  FROM base
),
binned AS (
  SELECT
    mm.vmin,
    mm.vmax,
    mm.n,
    CASE
      WHEN mm.vmax = mm.vmin THEN 0
      ELSE CAST(FLOOR( (value - mm.vmin) / ((mm.vmax - mm.vmin) / ${bins}) ) AS BIGINT)
    END AS bin
  FROM base, mm
)
SELECT
  vmin,
  vmax,
  n,
  CASE WHEN bin < 0 THEN 0 WHEN bin >= ${bins} THEN ${bins}-1 ELSE bin END AS bin,
  COUNT(*)::BIGINT AS count
FROM binned
GROUP BY vmin, vmax, n, bin
ORDER BY bin;
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-hist-freq",
      sqlSelect,
    });

    return res.json({
      domain: "freq",
      source: "s3_raw",
      time_col: RAW_TIME_COL,
      metric,
      bins,
      from: from.toISOString(),
      to: to.toISOString(),
      filters,
      s3_keys: keys,
      rows,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

// ---- SUMMARY (COLD S3 AGG) ----
// /telemetry/summary?domain=accel|freq&granularity=daily&from=...&to=...
export async function telemetrySummary(req, res) {
  try {
    const domain = mustOneOf((req.query.domain || "accel").toLowerCase(), ["accel", "freq"], "domain");
    const granularity = mustOneOf(req.query.granularity, ["hourly", "daily", "monthly"], "granularity");

    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const keys = keysForAggRange({ domain, granularity, from, to });

    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
    };

    const whereSql = buildWhere(filters);

    // bucket_br está em BR => filtra com from/to convertidos BR
    const fromTs = toDuckTsBR(from);
    const toTs = toDuckTsBR(to);

    const timeClause =
      (whereSql ? " AND " : "WHERE ") +
      `${AGG_TIME_COL} >= TIMESTAMP '${fromTs}' AND ${AGG_TIME_COL} < TIMESTAMP '${toTs}'`;

    const maxItems = Math.min(Math.max(Number(req.query.max || 500), 50), 2000);
    const dimCol = domain === "accel" ? "axis" : "stream";

    const sqlSelect = `
WITH base AS (
  SELECT device_id, ${dimCol} AS dim
  FROM v
  ${whereSql}${timeClause}
)
SELECT
  COUNT(*)::BIGINT AS rows_total,
  (SELECT COUNT(DISTINCT device_id)::BIGINT FROM base) AS devices_total,
  (SELECT COUNT(DISTINCT dim)::BIGINT FROM base) AS dims_total,
  (SELECT LIST(DISTINCT device_id) FROM (SELECT DISTINCT device_id FROM base LIMIT ${maxItems})) AS devices_sample,
  (SELECT LIST(DISTINCT dim) FROM (SELECT DISTINCT dim FROM base LIMIT ${maxItems})) AS dims_sample
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-summary",
      sqlSelect,
    });

    return res.json({
      domain,
      source: "s3_agg",
      granularity,
      from: from.toISOString(),
      to: to.toISOString(),
      filters,
      s3_keys: keys,
      result: rows?.[0] || null,
      note: `devices_sample/dims_sample limitados a ${maxItems} itens (use max=...)`,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}