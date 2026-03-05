// src/scripts/checkDayS3_samples.js
import "dotenv/config";
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

function s3Key(domain, type, dayBr) {
  const [Y, M] = dayBr.split("-");
  if (type === "raw") {
    return `telemetry_${domain}/raw/${Y}/${M}/${domain}_raw_${dayBr}.parquet`;
  }
  return `telemetry_${domain}/agg/${Y}/${M}/${type}/${domain}_${type}_${dayBr}.parquet`;
}

function runDuck(sql) {
  const cmd = spawnSync("duckdb", ["-c", sql], { encoding: "utf-8" });
  if (cmd.error) throw cmd.error;
  if (cmd.status !== 0) throw new Error(cmd.stderr || cmd.stdout);
  return cmd.stdout;
}

function saveTmpFile(buf, filename) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "d2win-check-"));
  const full = path.join(dir, filename);
  fs.writeFileSync(full, buf);
  return { dir, full };
}

function parseDescribeColumns(describeOutput) {
  // pega nomes da primeira coluna do DESCRIBE
  // output do duckdb vem em tabela; a coluna column_name aparece como primeira coluna.
  const lines = describeOutput.split("\n").map((l) => l.trim()).filter(Boolean);
  const cols = [];

  for (const l of lines) {
    // ignora linhas de borda
    if (l.startsWith("┌") || l.startsWith("└") || l.startsWith("├") || l.startsWith("│ column_name")) continue;
    // linhas "│ ts │ TIMESTAMP │ ..."
    if (l.startsWith("│")) {
      const parts = l.split("│").map((p) => p.trim()).filter(Boolean);
      if (parts.length >= 2) cols.push(parts[0]);
    }
  }
  return cols;
}

function pickTimeCol(cols) {
  // prioridade: bucket_br (agg), depois ts (raw), depois ts_br se for timestamp
  const preferred = ["bucket_br", "ts"];
  for (const c of preferred) if (cols.includes(c)) return c;
  // fallback: qualquer uma
  if (cols.includes("ts_br")) return "ts_br";
  return null;
}

function printBlock(title, txt) {
  console.log("\n" + "=".repeat(80));
  console.log(title);
  console.log("=".repeat(80));
  console.log(txt.trim());
}

async function inspect({ title, key }) {
  console.log(`\n[check] baixando s3://${mustEnv("S3_BUCKET")}/${key}`);
  const buf = await getObjectBuffer(key);
  console.log(`[check] size=${(buf.length / (1024 * 1024)).toFixed(2)} MB`);

  const { dir, full } = saveTmpFile(buf, path.basename(key));
  const file = full.replace(/\\/g, "\\\\");

  try {
    const desc = runDuck(`DESCRIBE SELECT * FROM read_parquet('${file}');`);
    printBlock(`${title} :: SCHEMA`, desc);

    const cols = parseDescribeColumns(desc);
    const timeCol = pickTimeCol(cols);

    const count = runDuck(`SELECT COUNT(*) AS n FROM read_parquet('${file}');`);
    printBlock(`${title} :: COUNT`, count);

    if (timeCol) {
      const minmax = runDuck(`
SELECT
  MIN(${timeCol}) AS min_time,
  MAX(${timeCol}) AS max_time
FROM read_parquet('${file}');
`);
      printBlock(`${title} :: MIN/MAX (${timeCol})`, minmax);

      const head2 = runDuck(`
SELECT * FROM read_parquet('${file}')
ORDER BY ${timeCol} ASC
LIMIT 2;
`);
      printBlock(`${title} :: 2 primeiros (ORDER BY ${timeCol} ASC)`, head2);

      const tail2 = runDuck(`
SELECT * FROM read_parquet('${file}')
ORDER BY ${timeCol} DESC
LIMIT 2;
`);
      printBlock(`${title} :: 2 últimos (ORDER BY ${timeCol} DESC)`, tail2);
    } else {
      console.log(`[check] (aviso) não achei coluna de tempo (bucket_br/ts/ts_br).`);
      const sample = runDuck(`SELECT * FROM read_parquet('${file}') LIMIT 2;`);
      printBlock(`${title} :: SAMPLE (2 linhas)`, sample);
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

async function main() {
  const dayBr = process.argv[2] || "2026-03-01";
  console.log("[check] dia BR:", dayBr);

  const tasks = [
    { title: "RAW ACCEL", key: s3Key("accel", "raw", dayBr) },
    { title: "RAW FREQ", key: s3Key("freq", "raw", dayBr) },
    { title: "AGG HOURLY ACCEL", key: s3Key("accel", "hourly", dayBr) },
    { title: "AGG HOURLY FREQ", key: s3Key("freq", "hourly", dayBr) },
    { title: "AGG DAILY ACCEL", key: s3Key("accel", "daily", dayBr) },
    { title: "AGG DAILY FREQ", key: s3Key("freq", "daily", dayBr) },
  ];

  for (const t of tasks) {
    try {
      await inspect(t);
    } catch (e) {
      console.error(`\n[check] ERRO em ${t.title}`);
      console.error(String(e?.message || e));
    }
  }

  console.log("\n[check] fim");
}

main().catch((e) => {
  console.error("[check] fatal:", e);
  process.exit(1);
});