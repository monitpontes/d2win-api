// ETL/agg_accel_month.js
// Agregação INCREMENTAL por mês (YYYY-MM) para aceleração.
// Lê apenas:    ETL/tmp/accel_YYYY-MM.parquet
// Gera (sobrescreve só o mês):
//   ETL/tmp/agg/hourly/accel_hourly_YYYY-MM.parquet
//   ETL/tmp/agg/daily/accel_daily_YYYY-MM.parquet
//   ETL/tmp/agg/monthly/accel_monthly_YYYY-MM.parquet
//
// Usa buckets em horário BR via ts_br_ts (TIMESTAMP).
//
// Uso:
//   node ETL/agg_accel_month.js 2025-10

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

function parseYYYYMM(s) {
  if (!/^\d{4}-\d{2}$/.test(s)) {
    throw new Error(`Formato inválido. Use YYYY-MM. Ex: 2026-02 (recebido: ${s})`);
  }
  const [y, m] = s.split("-").map(Number);
  if (m < 1 || m > 12) throw new Error("Mês inválido.");
  return { y, m };
}

function escapeWin(p) {
  return p.replaceAll("\\", "\\\\");
}

async function main() {
  const yyyymm = process.argv[2];
  if (!yyyymm) {
    console.error("Uso: node ETL/agg_accel_month.js YYYY-MM   (ex: 2026-02)");
    process.exit(1);
  }
  parseYYYYMM(yyyymm); // valida formato

  const DUCKDB_CLI = mustEnv("DUCKDB_CLI");

  const rawPath = path.resolve(`ETL/tmp/accel_${yyyymm}.parquet`);
  if (!fs.existsSync(rawPath)) {
    console.error("Arquivo RAW não encontrado:", rawPath);
    console.error("Gere primeiro com: node ETL/export_accel_month.js", yyyymm);
    process.exit(1);
  }

  // pastas de saída
  const aggBase = path.resolve("ETL/tmp/agg");
  const outHourlyDir = path.join(aggBase, "hourly");
  const outDailyDir = path.join(aggBase, "daily");
  const outMonthlyDir = path.join(aggBase, "monthly");

  fs.mkdirSync(outHourlyDir, { recursive: true });
  fs.mkdirSync(outDailyDir, { recursive: true });
  fs.mkdirSync(outMonthlyDir, { recursive: true });

  const outHourly = path.join(outHourlyDir, `accel_hourly_${yyyymm}.parquet`);
  const outDaily = path.join(outDailyDir, `accel_daily_${yyyymm}.parquet`);
  const outMonthly = path.join(outMonthlyDir, `accel_monthly_${yyyymm}.parquet`);

  console.log("Mês:", yyyymm);
  console.log("RAW:", rawPath);
  console.log("Saídas:");
  console.log(" - hourly:", outHourly);
  console.log(" - daily :", outDaily);
  console.log(" - monthly:", outMonthly);

  const inP = escapeWin(rawPath);
  const outH = escapeWin(outHourly);
  const outD = escapeWin(outDaily);
  const outM = escapeWin(outMonthly);

  const sql = `
PRAGMA enable_progress_bar;

-- Lê apenas 1 arquivo do mês
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
FROM read_parquet('${inP}');

-- Se existirem linhas sem ts_br_ts (NULL), elas quebram bucket. Vamos remover.
CREATE OR REPLACE VIEW accel_clean AS
SELECT * FROM accel_raw WHERE ts_br_ts IS NOT NULL;

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
  FROM accel_clean
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
  FROM accel_clean
  GROUP BY 1,2,3,4,5
  ORDER BY company_id, bridge_id, device_id, axis, day_br
) TO '${outD}' (FORMAT PARQUET);

-- =========================
-- POR MÊS (BR)
-- (nesse caso vai dar 1 mês mesmo, mas mantém padrão)
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
  FROM accel_clean
  GROUP BY 1,2,3,4,5
  ORDER BY company_id, bridge_id, device_id, axis, month_br
) TO '${outM}' (FORMAT PARQUET);

SELECT 'OK' AS status;
`;

  const r = spawnSync(DUCKDB_CLI, [":memory:", "-c", sql], { encoding: "utf-8" });
  if (r.status !== 0) {
    throw new Error(`DuckDB falhou:\n${r.stderr || r.stdout}`);
  }

  console.log("✅ OK! Agregações geradas (sobrescrevendo só esse mês).");
}

main().catch((e) => {
  console.error("Erro:", e);
  process.exit(1);
});