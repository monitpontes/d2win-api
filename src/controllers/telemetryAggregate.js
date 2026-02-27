import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { spawnSync } from "node:child_process";
import { getObjectBuffer } from "../services/s3Objects.js";

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} ausente no .env`);
  return v;
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

function mustOneOf(v, allowed, name) {
  if (!allowed.includes(v)) throw new Error(`${name} inválido. Use: ${allowed.join(", ")}`);
  return v;
}

// evita SQL injection nos filtros simples
function safeToken(v, fieldName) {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s) return null;
  // permite letras/números e alguns separadores comuns dos seus ids (Motiva_P1_S01, 68b9..., freq:z)
  if (!/^[A-Za-z0-9._:-]+$/.test(s)) {
    throw new Error(`Valor inválido em ${fieldName}`);
  }
  return s;
}

function buildAggKey({ domain, granularity, year, month }) {
  const yyyy = String(year);
  const mm = pad2(month);
  const yyyymm = `${yyyy}-${mm}`;

  if (domain === "accel") {
    return `telemetry_accel/agg/${yyyy}/${mm}/${granularity}/accel_${granularity}_${yyyymm}.parquet`;
  }
  return `telemetry_freq/agg/${yyyy}/${mm}/${granularity}/freq_${granularity}_${yyyymm}.parquet`;
}

function writeTmpFile(buffer, filename) {
  // Render: /tmp é ok
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "d2win-agg-"));
  const out = path.join(dir, filename);
  fs.writeFileSync(out, buffer);
  return out;
}

function escPath(p) {
  // funciona no Windows e Linux
  return p.replaceAll("\\", "\\\\");
}

function duckdbQueryJson({ parquetLocalPath, sqlSelect }) {
  const duckdbCli = mustEnv("DUCKDB_CLI");
  const p = escPath(parquetLocalPath);

  // COPY TO STDOUT JSON -> parse
  const sql = `
PRAGMA threads=4;

CREATE OR REPLACE VIEW v AS
SELECT * FROM read_parquet('${p}');

COPY (
  ${sqlSelect}
) TO STDOUT (FORMAT JSON);
`;

  const r = spawnSync(duckdbCli, [":memory:", "-c", sql], {
    encoding: "utf-8",
    maxBuffer: 100 * 1024 * 1024,
  });

  if (r.status !== 0) {
    throw new Error(`DuckDB falhou:\n${r.stderr || r.stdout}`);
  }

  const text = (r.stdout || "").trim();
  if (!text) return [];
  return JSON.parse(text);
}

function buildWhere(filters) {
  const clauses = [];
  for (const [col, value] of Object.entries(filters)) {
    if (!value) continue;
    // value já é safeToken -> pode interpolar como string literal
    clauses.push(`${col}='${value}'`);
  }
  return clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
}

/* =========================
   ACCEL AGG
   ========================= */
export async function accelAggregate(req, res) {
  try {
    const granularity = mustOneOf(req.query.granularity, ["hourly", "daily", "monthly"], "granularity");

    const year = Number(req.query.year);
    const month = Number(req.query.month);
    if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
      return res.status(400).json({ error: "year e month são obrigatórios (ex: year=2025&month=11)" });
    }

    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
      device_id: safeToken(req.query.device_id, "device_id"),
      axis: safeToken(req.query.axis, "axis"),
    };

    const key = buildAggKey({ domain: "accel", granularity, year, month });

    // baixa parquet do S3
    const buf = await getObjectBuffer(key);
    const localFile = writeTmpFile(buf, path.basename(key));

    const whereSql = buildWhere(filters);

    // Campos esperados do agg_accel_month.js:
    // company_id, bridge_id, device_id, axis, bucket_br,
    // value_avg, value_min, value_max, value_std, n_points, n_normal, n_alerta, n_critico
    const sqlSelect = `
SELECT
  company_id,
  bridge_id,
  device_id,
  axis,
  bucket_br,
  value_avg,
  value_min,
  value_max,
  value_std,
  n_points,
  n_normal,
  n_alerta,
  n_critico
FROM v
${whereSql}
ORDER BY bucket_br, device_id, axis
`;

    const rows = duckdbQueryJson({ parquetLocalPath: localFile, sqlSelect });

    return res.json({
      domain: "accel",
      granularity,
      year,
      month,
      s3_key: key,
      count: rows.length,
      rows,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

/* =========================
   FREQ AGG
   ========================= */
export async function freqAggregate(req, res) {
  try {
    const granularity = mustOneOf(req.query.granularity, ["hourly", "daily", "monthly"], "granularity");

    const year = Number(req.query.year);
    const month = Number(req.query.month);
    if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
      return res.status(400).json({ error: "year e month são obrigatórios (ex: year=2025&month=11)" });
    }

    const filters = {
      company_id: safeToken(req.query.company_id, "company_id"),
      bridge_id: safeToken(req.query.bridge_id, "bridge_id"),
      device_id: safeToken(req.query.device_id, "device_id"),
      stream: safeToken(req.query.stream, "stream"),
    };

    const key = buildAggKey({ domain: "freq", granularity, year, month });

    const buf = await getObjectBuffer(key);
    const localFile = writeTmpFile(buf, path.basename(key));

    const whereSql = buildWhere(filters);

    // Campos esperados do agg_freq_month.js:
    // company_id, bridge_id, device_id, stream, bucket_br,
    // f1_avg, mag1_avg, f2_avg, mag2_avg, n_points, n_has_f1, n_has_f2, n_atividade,
    // peak_f_max, peak_mag_of_max_f, peak_tag_of_max_f
    const sqlSelect = `
SELECT
  company_id,
  bridge_id,
  device_id,
  stream,
  bucket_br,
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
${whereSql}
ORDER BY bucket_br, device_id, stream
`;

    const rows = duckdbQueryJson({ parquetLocalPath: localFile, sqlSelect });

    return res.json({
      domain: "freq",
      granularity,
      year,
      month,
      s3_key: key,
      count: rows.length,
      rows,
    });
  } catch (e) {
    return res.status(500).json({ error: String(e?.message || e) });
  }
}