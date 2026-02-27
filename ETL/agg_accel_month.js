// ETL/agg_accel_month.js
// Uso: node ETL/agg_accel_month.js 2025-10

import dotenv from "dotenv";
import fs from "node:fs";
import { spawnSync } from "node:child_process";
import { parseYYYYMM, rawMonthPath, aggOutPath, escWin } from "./_paths.js";

dotenv.config();

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variável de ambiente ausente: ${name}`);
  return v;
}

async function main() {
  const yyyymm = process.argv[2];
  if (!yyyymm) {
    console.error("Uso: node ETL/agg_accel_month.js YYYY-MM   (ex: 2026-02)");
    process.exit(1);
  }
  parseYYYYMM(yyyymm);

  const DUCKDB_CLI = mustEnv("DUCKDB_CLI");

  const rawPath = rawMonthPath("telemetry_accel", yyyymm);
  if (!fs.existsSync(rawPath)) {
    console.error("Arquivo RAW não encontrado:", rawPath);
    console.error("Gere primeiro com: node ETL/export_accel_month.js", yyyymm);
    process.exit(1);
  }

  const outHourly = aggOutPath("telemetry_accel", yyyymm, "hourly");
  const outDaily = aggOutPath("telemetry_accel", yyyymm, "daily");
  const outMonthly = aggOutPath("telemetry_accel", yyyymm, "monthly");

  console.log("Mês:", yyyymm);
  console.log("RAW:", rawPath);
  console.log("Saídas:");
  console.log(" - hourly :", outHourly);
  console.log(" - daily  :", outDaily);
  console.log(" - monthly:", outMonthly);

  const inP = escWin(rawPath);
  const outH = escWin(outHourly);
  const outD = escWin(outDaily);
  const outM = escWin(outMonthly);

  const sql = `
PRAGMA enable_progress_bar;

CREATE OR REPLACE VIEW accel_raw AS
SELECT
  ts_br_ts,
  company_id,
  bridge_id,
  device_id,
  axis,
  severity,
  value
FROM read_parquet('${inP}');

CREATE OR REPLACE VIEW accel_clean AS
SELECT * FROM accel_raw WHERE ts_br_ts IS NOT NULL;

-- POR HORA (BR)
COPY (
  SELECT
    company_id,
    bridge_id,
    device_id,
    axis,
    date_trunc('hour', ts_br_ts) AS bucket_br,

    avg(value) AS value_avg,
    min(value) AS value_min,
    max(value) AS value_max,
    stddev_pop(value) AS value_std,

    count(*) AS n_points,

    sum(CASE WHEN severity='normal' THEN 1 ELSE 0 END) AS n_normal,
    sum(CASE WHEN severity='alerta' THEN 1 ELSE 0 END) AS n_alerta,
    sum(CASE WHEN severity='critico' THEN 1 ELSE 0 END) AS n_critico
  FROM accel_clean
  GROUP BY 1,2,3,4,5
  ORDER BY 1,2,3,4,5
) TO '${outH}' (FORMAT PARQUET);

-- POR DIA (BR)
COPY (
  SELECT
    company_id,
    bridge_id,
    device_id,
    axis,
    date_trunc('day', ts_br_ts) AS bucket_br,

    avg(value) AS value_avg,
    min(value) AS value_min,
    max(value) AS value_max,
    stddev_pop(value) AS value_std,

    count(*) AS n_points,

    sum(CASE WHEN severity='normal' THEN 1 ELSE 0 END) AS n_normal,
    sum(CASE WHEN severity='alerta' THEN 1 ELSE 0 END) AS n_alerta,
    sum(CASE WHEN severity='critico' THEN 1 ELSE 0 END) AS n_critico
  FROM accel_clean
  GROUP BY 1,2,3,4,5
  ORDER BY 1,2,3,4,5
) TO '${outD}' (FORMAT PARQUET);

-- POR MÊS (BR)
COPY (
  SELECT
    company_id,
    bridge_id,
    device_id,
    axis,
    date_trunc('month', ts_br_ts) AS bucket_br,

    avg(value) AS value_avg,
    min(value) AS value_min,
    max(value) AS value_max,
    stddev_pop(value) AS value_std,

    count(*) AS n_points,

    sum(CASE WHEN severity='normal' THEN 1 ELSE 0 END) AS n_normal,
    sum(CASE WHEN severity='alerta' THEN 1 ELSE 0 END) AS n_alerta,
    sum(CASE WHEN severity='critico' THEN 1 ELSE 0 END) AS n_critico
  FROM accel_clean
  GROUP BY 1,2,3,4,5
  ORDER BY 1,2,3,4,5
) TO '${outM}' (FORMAT PARQUET);

SELECT 'OK' AS status;
`;

  const r = spawnSync(DUCKDB_CLI, [":memory:", "-c", sql], { encoding: "utf-8" });
  if (r.status !== 0) throw new Error(`DuckDB falhou:\n${r.stderr || r.stdout}`);

  console.log("✅ OK! Agregações geradas (sobrescrevendo só esse mês).");
  console.log("Obs: coluna bucket_br é (hour/day/month) conforme o arquivo.");
}

main().catch((e) => {
  console.error("Erro:", e);
  process.exit(1);
});