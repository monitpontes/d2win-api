// ETL/export_accel_month.js
// Backfill mensal (YYYY-MM) usando filtro por ts (Date UTC) no Mongo.
// Gera Parquet em ETL/tmp/accel_YYYY-MM.parquet com:
// - ts_utc (TIMESTAMPTZ)
// - ts_br_ts (TIMESTAMP, horário BR, sem fuso)
// - ts_raw (string ISO) e ts_br_raw (string original)
// Uso: node ETL/export_accel_month.js 2025-10

import dotenv from "dotenv";
import path from "node:path";
import fs from "node:fs";
import os from "node:os";
import { MongoClient } from "mongodb";
import { spawnSync } from "node:child_process";

dotenv.config(); // rode da raiz para ler .env

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variável de ambiente ausente: ${name}`);
  return v;
}

function resolveDbNameFromUri(uri) {
  try {
    const u = new URL(uri);
    const p = (u.pathname || "").replace("/", "").trim();
    if (p) return p;
  } catch (_) {}
  return process.env.MONGO_DB || "test";
}

function oidToString(x) {
  if (!x) return null;
  if (typeof x === "string") return x;
  if (typeof x.toString === "function") return x.toString();
  return String(x);
}

function normalizeAccel(doc) {
  return {
    ts_raw: doc.ts,                 // Date do Mongo -> vai virar ISO no NDJSON
    ts_br_raw: doc.ts_br ?? null,   // string BR

    company_id: oidToString(doc?.meta?.company_id),
    bridge_id: oidToString(doc?.meta?.bridge_id),
    device_id: doc.device_id ?? doc?.meta?.device_id ?? null,

    axis: doc?.meta?.axis ?? null,
    severity: doc.severity ?? null,
    value: doc.value ?? null,
  };
}

function parseYYYYMM(s) {
  if (!/^\d{4}-\d{2}$/.test(s)) {
    throw new Error(`Formato inválido. Use YYYY-MM. Ex: 2026-02 (recebido: ${s})`);
  }
  const [y, m] = s.split("-").map(Number);
  if (m < 1 || m > 12) throw new Error("Mês inválido.");
  return { y, m };
}

function monthStartUTC(yyyymm) {
  const { y, m } = parseYYYYMM(yyyymm);
  return new Date(Date.UTC(y, m - 1, 1, 0, 0, 0));
}

function monthEndUTC(yyyymm) {
  const { y, m } = parseYYYYMM(yyyymm);
  const next = m === 12 ? { y: y + 1, m: 1 } : { y, m: m + 1 };
  return new Date(Date.UTC(next.y, next.m - 1, 1, 0, 0, 0));
}

async function connectWithRetry(client, tries = 3) {
  let lastErr;
  for (let i = 1; i <= tries; i++) {
    try {
      await client.connect();
      return;
    } catch (e) {
      lastErr = e;
      console.log(`Falhou conectar (tentativa ${i}/${tries}). Tentando novamente...`);
      await new Promise((r) => setTimeout(r, 2000 * i));
    }
  }
  throw lastErr;
}

async function main() {
  const yyyymm = process.argv[2];
  if (!yyyymm) {
    console.error("Uso: node ETL/export_accel_month.js YYYY-MM   (ex: 2026-02)");
    process.exit(1);
  }

  const MONGO_URI = mustEnv("MONGO_URI");
  const DUCKDB_CLI = mustEnv("DUCKDB_CLI");
  const dbName = resolveDbNameFromUri(MONGO_URI);

  const start = monthStartUTC(yyyymm);
  const end = monthEndUTC(yyyymm);

  const tmpDir = path.resolve("ETL/tmp");
  fs.mkdirSync(tmpDir, { recursive: true });

  const tmpBase = fs.mkdtempSync(path.join(os.tmpdir(), "d2win-etl-"));
  const ndjsonPath = path.join(tmpBase, `accel_${yyyymm}.ndjson`);
  const outParquet = path.join(tmpDir, `accel_${yyyymm}.parquet`);

  console.log("DB selecionado:", dbName);
  console.log("Mês:", yyyymm);
  console.log("Faixa ts (UTC):", start.toISOString(), "->", end.toISOString());
  console.log("[1/3] Buscando Mongo...");

  const client = new MongoClient(MONGO_URI, {
    serverSelectionTimeoutMS: 60000,
    connectTimeoutMS: 60000,
    socketTimeoutMS: 0,
    maxPoolSize: 5,
  });

  let count = 0;
  await connectWithRetry(client, 3);

  try {
    const db = client.db(dbName);
    const col = db.collection("telemetry_ts_accel");

    // Filtro por ts (Date)
    const cursor = col.find(
      { ts: { $gte: start, $lt: end } },
      { projection: { _id: 0 }, batchSize: 5000 }
    );

    const fd = fs.openSync(ndjsonPath, "w");
    try {
      for await (const doc of cursor) {
        fs.writeSync(fd, JSON.stringify(normalizeAccel(doc)) + "\n");
        count++;
        if (count % 200000 === 0) console.log("  ...", count, "docs");
      }
    } finally {
      fs.closeSync(fd);
    }
  } finally {
    await client.close();
  }

  console.log("  docs:", count);
  if (count === 0) {
    console.log("Nada para exportar nesse mês.");
    return;
  }

  console.log("[2/3] Gerando Parquet com DuckDB (tipando timestamps)...");

  // escape paths Windows
  const nd = ndjsonPath.replaceAll("\\", "\\\\");
  const out = outParquet.replaceAll("\\", "\\\\");

  const sql = `
CREATE OR REPLACE TABLE t AS
SELECT
  CAST(ts_raw AS TIMESTAMPTZ) AS ts_utc,
  CAST(ts_br_raw AS TIMESTAMP) AS ts_br_ts,

  CAST(ts_raw AS VARCHAR) AS ts_raw,
  ts_br_raw,

  company_id,
  bridge_id,
  device_id,
  axis,
  severity,
  value
FROM read_json_auto('${nd}');

COPY t TO '${out}' (FORMAT PARQUET);
`;

  const r = spawnSync(DUCKDB_CLI, [":memory:", "-c", sql], { encoding: "utf-8" });
  if (r.status !== 0) {
    throw new Error(`DuckDB falhou:\n${r.stderr || r.stdout}`);
  }

  console.log("[3/3] OK ✅ Parquet criado em:", outParquet);
}

main().catch((e) => {
  console.error("Erro:", e);
  process.exit(1);
});