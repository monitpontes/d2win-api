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
  duckdbSchemaFromS3Keys,
} from "../lib/s3Telemetry.js";

/**
 * GET /telemetry/accel/schema?from=...&to=...&source=raw|agg
 */
export async function accelSchema(req, res) {
  try {
    const source = (req.query.source || "raw").toLowerCase();
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const keys =
      source === "agg"
        ? keysForAggRange({ domain: "accel", granularity: mustOneOf(req.query.granularity || "hourly", ["hourly","daily","monthly"], "granularity"), from, to })
        : keysForRawRange({ domain: "accel", from, to });

    const columns = await duckdbSchemaFromS3Keys({ keys, tmpPrefix: "d2win-schema-accel" });
    return res.json({ domain: "accel", source: `s3_${source}`, s3_keys: keys, columns });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

/**
 * GET /telemetry/freq/schema?from=...&to=...&source=raw|agg&granularity=hourly|daily|monthly
 */
export async function freqSchema(req, res) {
  try {
    const source = (req.query.source || "raw").toLowerCase();
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const keys =
      source === "agg"
        ? keysForAggRange({ domain: "freq", granularity: mustOneOf(req.query.granularity || "hourly", ["hourly","daily","monthly"], "granularity"), from, to })
        : keysForRawRange({ domain: "freq", from, to });

    const columns = await duckdbSchemaFromS3Keys({ keys, tmpPrefix: "d2win-schema-freq" });
    return res.json({ domain: "freq", source: `s3_${source}`, s3_keys: keys, columns });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

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
      time_col: AGG_TIME_COL,
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

    // AGG freq TEM as colunas f1_avg/mag1_avg/f2_avg/mag2_avg etc
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
      time_col: AGG_TIME_COL,
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
 * device_id obrigatório
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

    const fromTs = toDuckTsBR(from);
    const toTs = toDuckTsBR(to);

    const whereSql = buildWhereWithTime({
      filters,
      timeCol: RAW_TIME_COL,
      fromTs,
      toTs,
    });

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
 * GET /telemetry/freq/raw?from=...&to=...&device_id=...&stream=...&limit=20000&order=asc
 *
 * device_id obrigatório
 * ✅ f1/mag1/f2/mag2 extraídos de peaks_json
 */
export async function freqRawRange(req, res) {
  try {
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const device_id = safeToken(req.query.device_id, "device_id");
    if (!device_id) return res.status(400).json({ error: "device_id é obrigatório no RAW" });

    const stream = safeToken(req.query.stream, "stream"); // opcional
    const limit = Math.min(Number(req.query.limit || 20000), 100000);
    const order = mustOneOf((req.query.order || "asc").toLowerCase(), ["asc", "desc"], "order");

    const keys = keysForRawRange({ domain: "freq", from, to });

    const fromTs = toDuckTsBR(from);
    const toTs = toDuckTsBR(to);

    // filtro com tempo em ts_br_ts + device_id + stream opcional
    const whereSql = buildWhereWithTime({
      filters: {
        company_id: safeToken(req.query.company_id, "company_id"),
        bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
        device_id,
        stream,
      },
      timeCol: RAW_TIME_COL,
      fromTs,
      toTs,
    });

    // ✅ Extrai do peaks_json (string) igual seu DuckDB no terminal
    const sqlSelect = `
WITH base AS (
  SELECT
    company_id,
    bridge_id,
    device_id,
    stream,
    ${RAW_TIME_COL} AS ts_br_ts,
    status,
    severity,
    n,
    fs,
    peaks_json,

    TRY_CAST(json_extract(peaks_json, '$[0].f')   AS DOUBLE) AS f1,
    TRY_CAST(json_extract(peaks_json, '$[0].mag') AS DOUBLE) AS mag1,
    TRY_CAST(json_extract(peaks_json, '$[1].f')   AS DOUBLE) AS f2,
    TRY_CAST(json_extract(peaks_json, '$[1].mag') AS DOUBLE) AS mag2
  FROM v
  ${whereSql}
)
SELECT
  company_id,
  bridge_id,
  device_id,
  stream,
  ts_br_ts,
  status,
  severity,
  n,
  fs,
  f1,
  mag1,
  f2,
  mag2,
  peaks_json
FROM base
ORDER BY ts_br_ts ${order}
LIMIT ${limit}
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-raw-range-freq",
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
      note: "f1/mag1/f2/mag2 são extraídos de peaks_json (RAW não tem essas colunas).",
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

/**
 * EXTREMOS RAW - accel
 * GET /telemetry/accel/raw/extrema?from=...&to=...&device_id=...&axis=...
 */
export async function accelRawExtrema(req, res) {
  try {
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const device_id = safeToken(req.query.device_id, "device_id");
    if (!device_id) return res.status(400).json({ error: "device_id é obrigatório no RAW" });

    const axis = safeToken(req.query.axis, "axis");

    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
      device_id,
      axis,
    };

    const keys = keysForRawRange({ domain: "accel", from, to });

    const whereSql = buildWhereWithTime({
      filters,
      timeCol: RAW_TIME_COL,
      fromTs: toDuckTsBR(from),
      toTs: toDuckTsBR(to),
    });

    const sqlSelect = `
WITH base AS (
  SELECT ${RAW_TIME_COL} AS ts_br_ts, value
  FROM v
  ${whereSql}
)
SELECT
  COUNT(*)::BIGINT AS n_rows,
  MIN(value) AS min_value,
  arg_min(ts_br_ts, value) AS ts_min_value,
  MAX(value) AS max_value,
  arg_max(ts_br_ts, value) AS ts_max_value
FROM base
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-raw-extrema-accel",
      sqlSelect,
    });

    return res.json({
      domain: "accel",
      source: "s3_raw",
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

/**
 * EXTREMOS RAW - freq (f1/f2 e mag1/mag2 vêm de peaks_json)
 * GET /telemetry/freq/raw/extrema?from=...&to=...&device_id=...&stream=...
 */
export async function freqRawExtrema(req, res) {
  try {
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const device_id = safeToken(req.query.device_id, "device_id");
    if (!device_id) return res.status(400).json({ error: "device_id é obrigatório no RAW" });

    const stream = safeToken(req.query.stream, "stream");

    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
      device_id,
      stream,
    };

    const keys = keysForRawRange({ domain: "freq", from, to });

    const whereSql = buildWhereWithTime({
      filters,
      timeCol: RAW_TIME_COL,
      fromTs: toDuckTsBR(from),
      toTs: toDuckTsBR(to),
    });

    const sqlSelect = `
WITH base AS (
  SELECT
    ${RAW_TIME_COL} AS ts_br_ts,
    TRY_CAST(json_extract(peaks_json, '$[0].f')   AS DOUBLE) AS f1,
    TRY_CAST(json_extract(peaks_json, '$[0].mag') AS DOUBLE) AS mag1,
    TRY_CAST(json_extract(peaks_json, '$[1].f')   AS DOUBLE) AS f2,
    TRY_CAST(json_extract(peaks_json, '$[1].mag') AS DOUBLE) AS mag2
  FROM v
  ${whereSql}
)
SELECT
  COUNT(*)::BIGINT AS n_rows,

  MIN(f1) AS min_f1,
  arg_min(ts_br_ts, f1) AS ts_min_f1,
  MAX(f1) AS max_f1,
  arg_max(ts_br_ts, f1) AS ts_max_f1,

  MIN(f2) AS min_f2,
  arg_min(ts_br_ts, f2) AS ts_min_f2,
  MAX(f2) AS max_f2,
  arg_max(ts_br_ts, f2) AS ts_max_f2,

  MIN(mag1) AS min_mag1,
  arg_min(ts_br_ts, mag1) AS ts_min_mag1,
  MAX(mag1) AS max_mag1,
  arg_max(ts_br_ts, mag1) AS ts_max_mag1,

  MIN(mag2) AS min_mag2,
  arg_min(ts_br_ts, mag2) AS ts_min_mag2,
  MAX(mag2) AS max_mag2,
  arg_max(ts_br_ts, mag2) AS ts_max_mag2
FROM base
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-raw-extrema-freq",
      sqlSelect,
    });

    return res.json({
      domain: "freq",
      source: "s3_raw",
      from: from.toISOString(),
      to: to.toISOString(),
      filters,
      s3_keys: keys,
      result: rows?.[0] || null,
      note: "Extremos calculados a partir de peaks_json (f1/f2/mag1/mag2).",
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}