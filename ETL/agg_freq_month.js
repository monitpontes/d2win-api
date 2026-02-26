// ETL/agg_freq_month.js
// Agregação INCREMENTAL por mês (YYYY-MM) para frequência.
// Lê: ETL/tmp/freq_YYYY-MM.parquet
// Gera:
//   ETL/tmp/agg/hourly/freq_hourly_YYYY-MM.parquet
//   ETL/tmp/agg/daily/freq_daily_YYYY-MM.parquet
//   ETL/tmp/agg/monthly/freq_monthly_YYYY-MM.parquet
//
// Regras:
// - médias: f1, f2, mag1, mag2 por bucket
// - contagens: n_points, severities
// - máximos >=8Hz e >=9Hz dentro do bucket:
//     f_max8_bucket, mag_at_f_max8_bucket, tag_at_f_max8_bucket
//     f_max9_bucket, mag_at_f_max9_bucket, tag_at_f_max9_bucket
//
// Uso: node ETL/agg_freq_month.js 2026-02

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
    console.error("Uso: node ETL/agg_freq_month.js YYYY-MM   (ex: 2026-02)");
    process.exit(1);
  }
  parseYYYYMM(yyyymm);

  const DUCKDB_CLI = mustEnv("DUCKDB_CLI");

  const rawPath = path.resolve(`ETL/tmp/freq_${yyyymm}.parquet`);
  if (!fs.existsSync(rawPath)) {
    console.error("Arquivo RAW não encontrado:", rawPath);
    console.error("Gere primeiro com: node ETL/export_freq_month.js", yyyymm);
    process.exit(1);
  }

  const aggBase = path.resolve("ETL/tmp/agg");
  const outHourlyDir = path.join(aggBase, "hourly");
  const outDailyDir = path.join(aggBase, "daily");
  const outMonthlyDir = path.join(aggBase, "monthly");

  fs.mkdirSync(outHourlyDir, { recursive: true });
  fs.mkdirSync(outDailyDir, { recursive: true });
  fs.mkdirSync(outMonthlyDir, { recursive: true });

  const outHourly = path.join(outHourlyDir, `freq_hourly_${yyyymm}.parquet`);
  const outDaily = path.join(outDailyDir, `freq_daily_${yyyymm}.parquet`);
  const outMonthly = path.join(outMonthlyDir, `freq_monthly_${yyyymm}.parquet`);

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

  // Dica:
  // - f_max8/mag_max8/tag_max8 já vêm por linha (evento)
  // - no bucket, queremos o "maior f_max8" (que já é >=8), e magnitude/tag correspondentes
  // Fazemos isso com arg_max(mag_max8, f_max8) e arg_max(tag_max8, f_max8).
  const sql = `
PRAGMA enable_progress_bar;

CREATE OR REPLACE VIEW freq_raw AS
SELECT
  ts_utc,
  ts_br_ts,
  company_id,
  bridge_id,
  device_id,
  stream,
  status,
  severity,
  n,
  fs,

  f1, mag1,
  f2, mag2,

  f_max8, mag_max8, tag_max8,
  f_max9, mag_max9, tag_max9
FROM read_parquet('${inP}');

CREATE OR REPLACE VIEW freq_clean AS
SELECT * FROM freq_raw WHERE ts_br_ts IS NOT NULL;

-- =========================
-- HOURLY (BR)
-- =========================
COPY (
  SELECT
    company_id,
    bridge_id,
    device_id,
    stream,
    date_trunc('hour', ts_br_ts) AS hour_br,

    avg(f1)   AS f1_avg,
    avg(f2)   AS f2_avg,
    avg(mag1) AS mag1_avg,
    avg(mag2) AS mag2_avg,

    count(*) AS n_points,

    sum(CASE WHEN severity='normal' THEN 1 ELSE 0 END) AS n_normal,
    sum(CASE WHEN severity='alerta' THEN 1 ELSE 0 END) AS n_alerta,
    sum(CASE WHEN severity='critico' THEN 1 ELSE 0 END) AS n_critico,

    max(f_max8) AS f_max8_bucket,
    arg_max(mag_max8, f_max8) AS mag_at_f_max8_bucket,
    arg_max(tag_max8, f_max8) AS tag_at_f_max8_bucket,

    max(f_max9) AS f_max9_bucket,
    arg_max(mag_max9, f_max9) AS mag_at_f_max9_bucket,
    arg_max(tag_max9, f_max9) AS tag_at_f_max9_bucket
  FROM freq_clean
  GROUP BY 1,2,3,4,5
  ORDER BY company_id, bridge_id, device_id, stream, hour_br
) TO '${outH}' (FORMAT PARQUET);

-- =========================
-- DAILY (BR)
-- =========================
COPY (
  SELECT
    company_id,
    bridge_id,
    device_id,
    stream,
    date_trunc('day', ts_br_ts) AS day_br,

    avg(f1)   AS f1_avg,
    avg(f2)   AS f2_avg,
    avg(mag1) AS mag1_avg,
    avg(mag2) AS mag2_avg,

    count(*) AS n_points,

    sum(CASE WHEN severity='normal' THEN 1 ELSE 0 END) AS n_normal,
    sum(CASE WHEN severity='alerta' THEN 1 ELSE 0 END) AS n_alerta,
    sum(CASE WHEN severity='critico' THEN 1 ELSE 0 END) AS n_critico,

    max(f_max8) AS f_max8_bucket,
    arg_max(mag_max8, f_max8) AS mag_at_f_max8_bucket,
    arg_max(tag_max8, f_max8) AS tag_at_f_max8_bucket,

    max(f_max9) AS f_max9_bucket,
    arg_max(mag_max9, f_max9) AS mag_at_f_max9_bucket,
    arg_max(tag_max9, f_max9) AS tag_at_f_max9_bucket
  FROM freq_clean
  GROUP BY 1,2,3,4,5
  ORDER BY company_id, bridge_id, device_id, stream, day_br
) TO '${outD}' (FORMAT PARQUET);

-- =========================
-- MONTHLY (BR)
-- =========================
COPY (
  SELECT
    company_id,
    bridge_id,
    device_id,
    stream,
    date_trunc('month', ts_br_ts) AS month_br,

    avg(f1)   AS f1_avg,
    avg(f2)   AS f2_avg,
    avg(mag1) AS mag1_avg,
    avg(mag2) AS mag2_avg,

    count(*) AS n_points,

    sum(CASE WHEN severity='normal' THEN 1 ELSE 0 END) AS n_normal,
    sum(CASE WHEN severity='alerta' THEN 1 ELSE 0 END) AS n_alerta,
    sum(CASE WHEN severity='critico' THEN 1 ELSE 0 END) AS n_critico,

    max(f_max8) AS f_max8_bucket,
    arg_max(mag_max8, f_max8) AS mag_at_f_max8_bucket,
    arg_max(tag_max8, f_max8) AS tag_at_f_max8_bucket,

    max(f_max9) AS f_max9_bucket,
    arg_max(mag_max9, f_max9) AS mag_at_f_max9_bucket,
    arg_max(tag_max9, f_max9) AS tag_at_f_max9_bucket
  FROM freq_clean
  GROUP BY 1,2,3,4,5
  ORDER BY company_id, bridge_id, device_id, stream, month_br
) TO '${outM}' (FORMAT PARQUET);

SELECT 'OK' AS status;
`;

  const r = spawnSync(DUCKDB_CLI, [":memory:", "-c", sql], { encoding: "utf-8" });
  if (r.status !== 0) {
    throw new Error(`DuckDB falhou:\n${r.stderr || r.stdout}`);
  }

  console.log("✅ OK! Agregações de freq geradas (sobrescrevendo só esse mês).");
}

main().catch((e) => {
  console.error("Erro:", e);
  process.exit(1);
});