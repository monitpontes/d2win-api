// src/controllers/s3Telemetry.js
import {
  mustOneOf,
  safeToken,
  parseISODate,
  toDuckTsUTCFromBrInput,
  keysForAggRangeAuto,
  keysForRawRangeAuto,
  buildWhere,
  buildWhereWithTime,
  duckdbQueryJsonFromS3Keys,
  duckdbSchemaFromS3Keys,
} from "../lib/s3Telemetry.js";

function colsToSet(columns) {
  return new Set((columns || []).map((c) => c.name));
}

function pickRawTimeCol(colSet) {
  if (colSet.has("ts")) return "ts";           // daily novo accel/freq (se existir)
  if (colSet.has("ts_utc")) return "ts_utc";   // legacy
  if (colSet.has("ts_raw")) return "ts_raw";   // algum modelo legacy seu
  if (colSet.has("ts_br_ts")) return "ts_br_ts";
  return null;
}

function pickRawBrCol(colSet) {
  if (colSet.has("ts_br")) return "ts_br";         // daily
  if (colSet.has("ts_br_raw")) return "ts_br_raw"; // legacy variante
  if (colSet.has("ts_br_ts")) return "ts_br_ts";   // legacy timestamp
  return null;
}

function sqlTimeExprFor(colSet, timeCol, fromUtc, toUtc) {
  // timeCol TIMESTAMP => TIMESTAMP '...'
  // timeCol TIMESTAMPTZ => TIMESTAMPTZ '...+00:00'
  if (timeCol === "ts_utc") {
    return {
      fromExpr: `TIMESTAMPTZ '${fromUtc}+00:00'`,
      toExpr: `TIMESTAMPTZ '${toUtc}+00:00'`,
      by: "ts_utc(TIMESTAMPTZ)",
    };
  }
  return {
    fromExpr: `TIMESTAMP '${fromUtc}'`,
    toExpr: `TIMESTAMP '${toUtc}'`,
    by: "ts(UTC)",
  };
}

/**
 * GET /telemetry/accel/schema?from=...&to=...&source=raw|agg&granularity=hourly|daily|monthly
 */
export async function accelSchema(req, res) {
  try {
    const source = (req.query.source || "raw").toLowerCase();
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const keys =
      source === "agg"
        ? await keysForAggRangeAuto({
          domain: "accel",
          granularity: mustOneOf(req.query.granularity || "hourly", ["hourly", "daily", "monthly"], "granularity"),
          from,
          to,
        })
        : await keysForRawRangeAuto({ domain: "accel", from, to });

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
        ? await keysForAggRangeAuto({
          domain: "freq",
          granularity: mustOneOf(req.query.granularity || "hourly", ["hourly", "daily", "monthly"], "granularity"),
          from,
          to,
        })
        : await keysForRawRangeAuto({ domain: "freq", from, to });

    const columns = await duckdbSchemaFromS3Keys({ keys, tmpPrefix: "d2win-schema-freq" });
    return res.json({ domain: "freq", source: `s3_${source}`, s3_keys: keys, columns });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

/**
 * COLD AGG (range) - accel
 */
export async function accelAggRange(req, res) {
  try {
    const granularity = mustOneOf(req.query.granularity, ["hourly", "daily", "monthly"], "granularity");
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const filters = { device_id: safeToken(req.query.device_id, "device_id") };
    const keys = await keysForAggRangeAuto({ domain: "accel", granularity, from, to });

    const whereSql = buildWhere(filters);
    const timeCol = "bucket_br";

    // schema probe
    const cols = await duckdbSchemaFromS3Keys({ keys, tmpPrefix: "d2win-agg-schema-probe" });
    const bucketType = (cols.find((c) => c.name === timeCol)?.type || "").toUpperCase();

    let timeClause = "";
    let fromUtc = null;
    let toUtc = null;

    if (bucketType.startsWith("DATE")) {
      // DATE (legacy daily): filtra por DATE, sem +3h
      const fromDate = from.toISOString().slice(0, 10);
      const toDate = to.toISOString().slice(0, 10);

      timeClause =
        (whereSql ? " AND " : "WHERE ") +
        `${timeCol} >= DATE '${fromDate}' AND ${timeCol} < DATE '${toDate}'`;
    } else {
      // TIMESTAMP (daily novo / hourly): mantém +3h
      fromUtc = toDuckTsUTCFromBrInput(from);
      toUtc = toDuckTsUTCFromBrInput(to);

      timeClause =
        (whereSql ? " AND " : "WHERE ") +
        `${timeCol} >= TIMESTAMP '${fromUtc}' AND ${timeCol} < TIMESTAMP '${toUtc}'`;
    }

    const sqlSelect = `
SELECT *
FROM v
${whereSql}${timeClause}
ORDER BY ${timeCol} ASC
`;

    const rows = await duckdbQueryJsonFromS3Keys({ keys, tmpPrefix: "d2win-agg-range-accel", sqlSelect });

    return res.json({
      domain: "accel",
      source: "s3_agg",
      granularity,
      from: from.toISOString(),
      to: to.toISOString(),
      time_filter: {
        by: timeCol, from_utc: fromUtc, to_utc: toUtc, note: bucketType.startsWith("DATE")
          ? "bucket_br é DATE (legacy daily) — filtro por DATE sem +3h"
          : "bucket_br é TIMESTAMP — filtro por TIMESTAMP com +3h",
      },
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
 */
export async function freqAggRange(req, res) {
  try {
    const granularity = mustOneOf(req.query.granularity, ["hourly", "daily", "monthly"], "granularity");
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const filters = { device_id: safeToken(req.query.device_id, "device_id") };
    const keys = await keysForAggRangeAuto({ domain: "freq", granularity, from, to });

    const whereSql = buildWhere(filters);
    const timeCol = "bucket_br";

    // schema probe
    const cols = await duckdbSchemaFromS3Keys({ keys, tmpPrefix: "d2win-agg-schema-probe" });
    const bucketType = (cols.find((c) => c.name === timeCol)?.type || "").toUpperCase();

    let timeClause = "";
    let fromUtc = null;
    let toUtc = null;

    if (bucketType.startsWith("DATE")) {
      // DATE (legacy daily): filtra por DATE, sem +3h
      const fromDate = from.toISOString().slice(0, 10);
      const toDate = to.toISOString().slice(0, 10);

      timeClause =
        (whereSql ? " AND " : "WHERE ") +
        `${timeCol} >= DATE '${fromDate}' AND ${timeCol} < DATE '${toDate}'`;
    } else {
      // TIMESTAMP (daily novo / hourly): mantém +3h
      fromUtc = toDuckTsUTCFromBrInput(from);
      toUtc = toDuckTsUTCFromBrInput(to);

      timeClause =
        (whereSql ? " AND " : "WHERE ") +
        `${timeCol} >= TIMESTAMP '${fromUtc}' AND ${timeCol} < TIMESTAMP '${toUtc}'`;
    }

    const sqlSelect = `
SELECT *
FROM v
${whereSql}${timeClause}
ORDER BY ${timeCol} ASC
`;

    const rows = await duckdbQueryJsonFromS3Keys({ keys, tmpPrefix: "d2win-agg-range-freq", sqlSelect });

    return res.json({
      domain: "freq",
      source: "s3_agg",
      granularity,
      from: from.toISOString(),
      to: to.toISOString(),
      time_filter: {
        by: timeCol, from_utc: fromUtc, to_utc: toUtc, note: bucketType.startsWith("DATE")
          ? "bucket_br é DATE (legacy daily) — filtro por DATE sem +3h"
          : "bucket_br é TIMESTAMP — filtro por TIMESTAMP com +3h",
      },
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
 * GET /telemetry/accel/raw?from=...&to=...&device_id=...&limit=20000&order=asc
 */
export async function accelRawRange(req, res) {
  try {
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const allowAll = String(req.query.allow_all || "") === "1";

    const device_id = safeToken(req.query.device_id, "device_id");
    if (!allowAll && !device_id) {
      return res.status(400).json({ error: "device_id é obrigatório no RAW (ou use allow_all=1 para debug)" });
    }

    const limit = Math.min(Number(req.query.limit || 20000), 100000);
    const order = mustOneOf((req.query.order || "asc").toLowerCase(), ["asc", "desc"], "order");

    const keys = await keysForRawRangeAuto({ domain: "accel", from, to });

    const fromUtc = toDuckTsUTCFromBrInput(from);
    const toUtc = toDuckTsUTCFromBrInput(to);

    // mantém isso:
    const whereDevice = device_id ? `WHERE device_id='${device_id}'` : "";

    // ✅ schema probe (pra não dar Binder Error quando for só daily ou só legacy)
    const cols = await duckdbSchemaFromS3Keys({
      keys,
      tmpPrefix: "d2win-raw-schema-probe-accel",
    });
    const colSet = colsToSet(cols);

    // escolhe coluna de tempo real existente
    let tcol = null;
    if (colSet.has("ts")) tcol = "ts";           // daily novo
    else if (colSet.has("ts_utc")) tcol = "ts_utc"; // legacy
    else if (colSet.has("ts_raw")) tcol = "ts_raw"; // se existir em algum legado seu

    if (!tcol) {
      return res.status(500).json({
        error: "RAW accel: não achei coluna de tempo (ts/ts_utc/ts_raw). Rode /telemetry/accel/schema?source=raw pra ver.",
        s3_keys: keys,
      });
    }

    // escolhe coluna BR existente (se houver)
    let brcol = null;
    if (colSet.has("ts_br")) brcol = "ts_br";          // daily (varchar)
    else if (colSet.has("ts_br_raw")) brcol = "ts_br_raw"; // legacy variante (varchar)
    else if (colSet.has("ts_br_ts")) brcol = "ts_br_ts";   // legacy (timestamp)

    const timeExpr = `TRY_CAST(${tcol} AS TIMESTAMP)`;

    const brExpr = !brcol
      ? `NULL`
      : (brcol === "ts_br" || brcol === "ts_br_raw")
        ? `TRY_CAST(${brcol} AS VARCHAR)`
        : `STRFTIME(TRY_CAST(${brcol} AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')`;

    const axisExpr = colSet.has("axis") ? "axis" : "NULL AS axis";
    const metaExpr = colSet.has("meta") ? "meta" : "NULL AS meta";

    const sqlSelect = `
WITH base AS (
  SELECT
    device_id,
    ${timeExpr} AS ts_u,
    ${brExpr}   AS ts_br_out,
    value,
    severity,
    ${axisExpr},
    ${metaExpr}
  FROM v
  ${whereDevice}
),
filtered AS (
  SELECT *
  FROM base
  WHERE ts_u >= TIMESTAMP '${fromUtc}'
    AND ts_u <  TIMESTAMP '${toUtc}'
)
SELECT
  device_id,
  ts_u AS ts_utc,
  ts_br_out AS ts_br,
  value,
  severity,
  axis,
  meta
FROM filtered
ORDER BY ts_utc ${order}
LIMIT ${limit}
`;

    const rows = await duckdbQueryJsonFromS3Keys({
      keys,
      tmpPrefix: "d2win-raw-range-accel",
      sqlSelect,
    });

    return res.json({
      domain: "accel",
      source: "s3_raw",
      from: from.toISOString(),
      to: to.toISOString(),
      time_filter: {
        by: "ts_u(UTC)",
        from_utc: fromUtc,
        to_utc: toUtc,
        note: "range BR convertido para UTC (+3h) e filtrado em ts_u",
      },
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
 * GET /telemetry/freq/raw?from=...&to=...&device_id=...&limit=20000&order=asc&allow_all=1
 */
export async function freqRawRange(req, res) {
  try {
    const from = parseISODate(req.query.from, "from");
    const to = parseISODate(req.query.to, "to");
    if (to <= from) return res.status(400).json({ error: "to deve ser maior que from" });

    const allowAll = String(req.query.allow_all || "") === "1";

    const device_id = safeToken(req.query.device_id, "device_id");
    if (!allowAll && !device_id) {
      return res.status(400).json({ error: "device_id é obrigatório no RAW (ou use allow_all=1 para debug)" });
    }

    const limit = Math.min(Number(req.query.limit || 20000), 100000);
    const order = mustOneOf((req.query.order || "asc").toLowerCase(), ["asc", "desc"], "order");

    const keys = await keysForRawRangeAuto({ domain: "freq", from, to });

    // range BR -> UTC (+3h) para filtrar em ts_utc/ts
    const fromUtc = toDuckTsUTCFromBrInput(from);
    const toUtc = toDuckTsUTCFromBrInput(to);

    // ✅ filtro de device_id (se tiver)
const whereDevice = device_id ? `WHERE device_id='${device_id}'` : "";

// ✅ schema probe (pra não dar Binder Error quando for só daily ou só legacy)
const cols = await duckdbSchemaFromS3Keys({
  keys,
  tmpPrefix: "d2win-raw-schema-probe-freq",
});
const colSet = colsToSet(cols);

// tempo real existente
let tcol = null;
if (colSet.has("ts")) tcol = "ts";              // daily novo
else if (colSet.has("ts_utc")) tcol = "ts_utc"; // legacy
else if (colSet.has("ts_raw")) tcol = "ts_raw"; // variante legacy

if (!tcol) {
  return res.status(500).json({
    error: "RAW freq: não achei coluna de tempo (ts/ts_utc/ts_raw). Rode /telemetry/freq/schema?source=raw pra ver.",
    s3_keys: keys,
  });
}

// BR existente (se houver)
let brcol = null;
if (colSet.has("ts_br")) brcol = "ts_br";            // daily (varchar)
else if (colSet.has("ts_br_raw")) brcol = "ts_br_raw"; // legacy variante (varchar)
else if (colSet.has("ts_br_ts")) brcol = "ts_br_ts";   // legacy (timestamp)

const timeExpr = `TRY_CAST(${tcol} AS TIMESTAMP)`;

const brExpr = !brcol
  ? `NULL`
  : (brcol === "ts_br" || brcol === "ts_br_raw")
    ? `TRY_CAST(${brcol} AS VARCHAR)`
    : `STRFTIME(TRY_CAST(${brcol} AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')`;

// campos que podem não existir (evita Binder Error)
const statusExpr = colSet.has("status") ? "status" : "NULL AS status";
const severityExpr = colSet.has("severity") ? "severity" : "NULL AS severity";
const nExpr = colSet.has("n") ? "n" : "NULL AS n";
const fsExpr = colSet.has("fs") ? "fs" : "NULL AS fs";

// peaks pode ter nomes diferentes dependendo do parquet
let peaksCol = null;
if (colSet.has("peaks")) peaksCol = "peaks";           // daily
else if (colSet.has("peaks_json")) peaksCol = "peaks_json"; // legacy
else if (colSet.has("peaks_raw")) peaksCol = "peaks_raw";   // variante

const peaksExpr = peaksCol ? `TRY_CAST(${peaksCol} AS VARCHAR)` : `NULL`;

const metaExpr = colSet.has("meta") ? "meta" : "NULL AS meta";

const sqlSelect = `
WITH base AS (
  SELECT
    device_id,
    ${timeExpr} AS ts_u,
    ${brExpr}   AS ts_br_out,
    ${statusExpr},
    ${severityExpr},
    ${nExpr},
    ${fsExpr},
    ${peaksExpr} AS peaks_str,
    ${metaExpr}
  FROM v
  ${whereDevice}
),
filtered AS (
  SELECT *
  FROM base
  WHERE ts_u >= TIMESTAMP '${fromUtc}'
    AND ts_u <  TIMESTAMP '${toUtc}'
)
SELECT
  device_id,
  ts_u AS ts_utc,
  ts_br_out AS ts_br,
  status,
  severity,
  n,
  fs,

  TRY_CAST(json_extract(peaks_str, '$[0].f')   AS DOUBLE) AS f1,
  TRY_CAST(json_extract(peaks_str, '$[0].mag') AS DOUBLE) AS mag1,
  TRY_CAST(json_extract(peaks_str, '$[1].f')   AS DOUBLE) AS f2,
  TRY_CAST(json_extract(peaks_str, '$[1].mag') AS DOUBLE) AS mag2,

  meta,
  peaks_str
FROM filtered
ORDER BY ts_utc ${order}
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
      time_filter: {
        by: "ts_u(UTC)",
        from_utc: fromUtc,
        to_utc: toUtc,
        note: "range BR convertido para UTC (+3h) e filtrado em ts_u",
      },
      s3_keys: keys,
      count: rows.length,
      rows,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}