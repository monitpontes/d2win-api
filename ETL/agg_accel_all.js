import dotenv from "dotenv";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

dotenv.config();

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variável de ambiente ausente: ${name}`);
  return v;
}

function listAccelParquets() {
  const dir = path.resolve("ETL/tmp");
  if (!fs.existsSync(dir)) return [];
  const files = fs.readdirSync(dir);
  return files
    .filter((f) => /^accel_\d{4}-\d{2}\.parquet$/.test(f))
    .map((f) => path.join(dir, f));
}

function escapeWin(p) {
  return p.replaceAll("\\", "\\\\");
}

async function main() {
  const DUCKDB_CLI = mustEnv("DUCKDB_CLI");

  const rawFiles = listAccelParquets();
  if (rawFiles.length === 0) {
    console.log("Nenhum arquivo RAW encontrado em ETL/tmp (accel_YYYY-MM.parquet).");
    process.exit(0);
  }

  console.log("Arquivos RAW encontrados:", rawFiles.length);
  rawFiles.slice(0, 5).forEach((f) => console.log("  -", f));
  if (rawFiles.length > 5) console.log("  ...");

  const tmpDir = path.resolve("ETL/tmp");
  const aggDir = path.resolve("ETL/tmp/agg");
  fs.mkdirSync(tmpDir, { recursive: true });
  fs.mkdirSync(aggDir, { recursive: true });

  const outHourly = path.join(aggDir, "agg_accel_hourly.parquet");
  const outDaily = path.join(aggDir, "agg_accel_daily.parquet");
  const outMonthly = path.join(aggDir, "agg_accel_monthly.parquet");

  const inputs = rawFiles.map(escapeWin).join("','");
  const outH = escapeWin(outHourly);
  const outD = escapeWin(outDaily);
  const outM = escapeWin(outMonthly);

  const sql = `
PRAGMA enable_progress_bar;

CREATE OR REPLACE VIEW accel_raw AS
SELECT
  ts_utc,
  ts_br_ts,
  company_id,
  bridge_id,
  device_id,
  axis,
  severity,
  value
FROM read_parquet(['${inputs}']);

-- =========================
-- POR HORA (BR)
-- =========================
COPY (
  SELECT
    company_id,
    bridge_id,
    device_id,
    axis,
    date_trunc('hour', ts_br_ts) AS hour_br,

    avg(value) AS value_avg,
    min(value) AS value_min,
    max(value) AS value_max,
    stddev_pop(value) AS value_std,

    count(*) AS n_points,

    sum(CASE WHEN severity='normal' THEN 1 ELSE 0 END) AS n_normal,
    sum(CASE WHEN severity='alerta' THEN 1 ELSE 0 END) AS n_alerta,
    sum(CASE WHEN severity='critico' THEN 1 ELSE 0 END) AS n_critico
  FROM accel_raw
  GROUP BY 1,2,3,4,5
  ORDER BY company_id, bridge_id, device_id, axis, hour_br
) TO '${outH}' (FORMAT PARQUET);

-- =========================
-- POR DIA (BR)
-- =========================
COPY (
  SELECT
    company_id,
    bridge_id,
    device_id,
    axis,
    date_trunc('day', ts_br_ts) AS day_br,

    avg(value) AS value_avg,
    min(value) AS value_min,
    max(value) AS value_max,
    stddev_pop(value) AS value_std,

    count(*) AS n_points,

    sum(CASE WHEN severity='normal' THEN 1 ELSE 0 END) AS n_normal,
    sum(CASE WHEN severity='alerta' THEN 1 ELSE 0 END) AS n_alerta,
    sum(CASE WHEN severity='critico' THEN 1 ELSE 0 END) AS n_critico
  FROM accel_raw
  GROUP BY 1,2,3,4,5
  ORDER BY company_id, bridge_id, device_id, axis, day_br
) TO '${outD}' (FORMAT PARQUET);

-- =========================
-- POR MÊS (BR)
-- =========================
COPY (
  SELECT
    company_id,
    bridge_id,
    device_id,
    axis,
    date_trunc('month', ts_br_ts) AS month_br,

    avg(value) AS value_avg,
    min(value) AS value_min,
    max(value) AS value_max,
    stddev_pop(value) AS value_std,

    count(*) AS n_points,

    sum(CASE WHEN severity='normal' THEN 1 ELSE 0 END) AS n_normal,
    sum(CASE WHEN severity='alerta' THEN 1 ELSE 0 END) AS n_alerta,
    sum(CASE WHEN severity='critico' THEN 1 ELSE 0 END) AS n_critico
  FROM accel_raw
  GROUP BY 1,2,3,4,5
  ORDER BY company_id, bridge_id, device_id, axis, month_br
) TO '${outM}' (FORMAT PARQUET);

SELECT 'OK' AS status;
`;

  const r = spawnSync(DUCKDB_CLI, [":memory:", "-c", sql], { encoding: "utf-8" });
  if (r.status !== 0) {
    throw new Error(`DuckDB falhou:\n${r.stderr || r.stdout}`);
  }

  console.log("✅ Agregações geradas em ETL/tmp/agg:");
  console.log(" -", outHourly);
  console.log(" -", outDaily);
  console.log(" -", outMonthly);
}

main().catch((e) => {
  console.error("Erro:", e);
  process.exit(1);
});