// ETL/agg_freq_month.js
// Uso: node ETL/agg_freq_month.js 2025-10

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
    console.error("Uso: node ETL/agg_freq_month.js YYYY-MM   (ex: 2026-02)");
    process.exit(1);
  }
  parseYYYYMM(yyyymm);

  const DUCKDB_CLI = mustEnv("DUCKDB_CLI");

  const rawPath = rawMonthPath("telemetry_freq", yyyymm);
  if (!fs.existsSync(rawPath)) {
    console.error("Arquivo RAW não encontrado:", rawPath);
    console.error("Gere primeiro com: node ETL/export_freq_month.js", yyyymm);
    process.exit(1);
  }

  const outHourly = aggOutPath("telemetry_freq", yyyymm, "hourly");
  const outDaily = aggOutPath("telemetry_freq", yyyymm, "daily");
  const outMonthly = aggOutPath("telemetry_freq", yyyymm, "monthly");

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

-- 1) Lê raw parquet do mês
CREATE OR REPLACE VIEW freq_raw AS
SELECT
  ts_br_ts,
  company_id,
  bridge_id,
  device_id,
  stream,
  status,
  severity,
  n,
  fs,
  peaks_json
FROM read_parquet('${inP}');

-- 2) Limpa + extrai f1/mag1/f2/mag2 do peaks_json
-- peaks_json é string tipo: [{"f":3.44,"mag":625.2},{"f":3.50,"mag":588.4}]
CREATE OR REPLACE VIEW freq_clean AS
SELECT
  ts_br_ts,
  company_id,
  bridge_id,
  device_id,
  stream,
  status,
  severity,
  n,
  fs,

  -- pico 1
  TRY_CAST(json_extract(peaks_json, '$[0].f')   AS DOUBLE) AS f1,
  TRY_CAST(json_extract(peaks_json, '$[0].mag') AS DOUBLE) AS mag1,

  -- pico 2
  TRY_CAST(json_extract(peaks_json, '$[1].f')   AS DOUBLE) AS f2,
  TRY_CAST(json_extract(peaks_json, '$[1].mag') AS DOUBLE) AS mag2
FROM freq_raw
WHERE ts_br_ts IS NOT NULL;

-- helper: max de freq no bucket considerando f1/f2, ignorando NULL
-- (DuckDB greatest(NULL, x) vira NULL, então usamos coalesce)
-- peak_f_max = max( coalesce(f1,-1), coalesce(f2,-1) ) no bucket

-- =========================
-- POR HORA (BR)
-- =========================
COPY (
  SELECT
    company_id,
    bridge_id,
    device_id,
    stream,
    date_trunc('hour', ts_br_ts) AS bucket_br,

    avg(f1)   AS f1_avg,
    avg(mag1) AS mag1_avg,
    avg(f2)   AS f2_avg,
    avg(mag2) AS mag2_avg,

    count(*) AS n_points,
    sum(CASE WHEN f1 IS NOT NULL THEN 1 ELSE 0 END) AS n_has_f1,
    sum(CASE WHEN f2 IS NOT NULL THEN 1 ELSE 0 END) AS n_has_f2,
    sum(CASE WHEN status='atividade_detectada' THEN 1 ELSE 0 END) AS n_atividade,

    -- max freq do bucket (entre f1 e f2)
    max(greatest(coalesce(f1,-1), coalesce(f2,-1))) AS peak_f_max,

    -- magnitude correspondente ao max freq do bucket:
    -- pegamos a linha do bucket onde a freq "ganhadora" é igual ao peak_f_max
    -- e dentro dela escolhemos mag1 ou mag2 conforme qual freq ganhou
    arg_max(
      CASE
        WHEN coalesce(f1,-1) >= coalesce(f2,-1) THEN mag1
        ELSE mag2
      END,
      greatest(coalesce(f1,-1), coalesce(f2,-1))
    ) AS peak_mag_of_max_f,

    arg_max(
      CASE
        WHEN coalesce(f1,-1) >= coalesce(f2,-1) THEN 'f1'
        ELSE 'f2'
      END,
      greatest(coalesce(f1,-1), coalesce(f2,-1))
    ) AS peak_tag_of_max_f

  FROM freq_clean
  GROUP BY 1,2,3,4,5
  ORDER BY 1,2,3,4,5
) TO '${outH}' (FORMAT PARQUET);

-- =========================
-- POR DIA (BR)
-- =========================
COPY (
  SELECT
    company_id,
    bridge_id,
    device_id,
    stream,
    date_trunc('day', ts_br_ts) AS bucket_br,

    avg(f1)   AS f1_avg,
    avg(mag1) AS mag1_avg,
    avg(f2)   AS f2_avg,
    avg(mag2) AS mag2_avg,

    count(*) AS n_points,
    sum(CASE WHEN f1 IS NOT NULL THEN 1 ELSE 0 END) AS n_has_f1,
    sum(CASE WHEN f2 IS NOT NULL THEN 1 ELSE 0 END) AS n_has_f2,
    sum(CASE WHEN status='atividade_detectada' THEN 1 ELSE 0 END) AS n_atividade,

    max(greatest(coalesce(f1,-1), coalesce(f2,-1))) AS peak_f_max,

    arg_max(
      CASE
        WHEN coalesce(f1,-1) >= coalesce(f2,-1) THEN mag1
        ELSE mag2
      END,
      greatest(coalesce(f1,-1), coalesce(f2,-1))
    ) AS peak_mag_of_max_f,

    arg_max(
      CASE
        WHEN coalesce(f1,-1) >= coalesce(f2,-1) THEN 'f1'
        ELSE 'f2'
      END,
      greatest(coalesce(f1,-1), coalesce(f2,-1))
    ) AS peak_tag_of_max_f

  FROM freq_clean
  GROUP BY 1,2,3,4,5
  ORDER BY 1,2,3,4,5
) TO '${outD}' (FORMAT PARQUET);

-- =========================
-- POR MÊS (BR)
-- =========================
COPY (
  SELECT
    company_id,
    bridge_id,
    device_id,
    stream,
    date_trunc('month', ts_br_ts) AS bucket_br,

    avg(f1)   AS f1_avg,
    avg(mag1) AS mag1_avg,
    avg(f2)   AS f2_avg,
    avg(mag2) AS mag2_avg,

    count(*) AS n_points,
    sum(CASE WHEN f1 IS NOT NULL THEN 1 ELSE 0 END) AS n_has_f1,
    sum(CASE WHEN f2 IS NOT NULL THEN 1 ELSE 0 END) AS n_has_f2,
    sum(CASE WHEN status='atividade_detectada' THEN 1 ELSE 0 END) AS n_atividade,

    max(greatest(coalesce(f1,-1), coalesce(f2,-1))) AS peak_f_max,

    arg_max(
      CASE
        WHEN coalesce(f1,-1) >= coalesce(f2,-1) THEN mag1
        ELSE mag2
      END,
      greatest(coalesce(f1,-1), coalesce(f2,-1))
    ) AS peak_mag_of_max_f,

    arg_max(
      CASE
        WHEN coalesce(f1,-1) >= coalesce(f2,-1) THEN 'f1'
        ELSE 'f2'
      END,
      greatest(coalesce(f1,-1), coalesce(f2,-1))
    ) AS peak_tag_of_max_f

  FROM freq_clean
  GROUP BY 1,2,3,4,5
  ORDER BY 1,2,3,4,5
) TO '${outM}' (FORMAT PARQUET);

SELECT 'OK' AS status;
`;

  const r = spawnSync(DUCKDB_CLI, [":memory:", "-c", sql], { encoding: "utf-8" });
  if (r.status !== 0) throw new Error(`DuckDB falhou:\n${r.stderr || r.stdout}`);

  console.log("✅ OK! Agregações geradas (sobrescrevendo só esse mês).");
  console.log("Obs: bucket_br é (hour/day/month) conforme o arquivo.");
}

main().catch((e) => {
  console.error("Erro:", e);
  process.exit(1);
});