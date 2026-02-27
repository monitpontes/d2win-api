// ETL/export_accel_month_br.js
// Export mensal (YYYY-MM) usando filtro por ts_br (string BR) no Mongo.
// Saída:
//   ETL/tmp/telemetry_accel/raw/YYYY/MM/accel_YYYY-MM.parquet
//
// Uso:
//   node ETL/export_accel_month_br.js 2025-11

import dotenv from "dotenv";
import path from "node:path";
import fs from "node:fs";
import os from "node:os";
import { MongoClient } from "mongodb";
import { spawnSync } from "node:child_process";

dotenv.config();

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
  } catch {}
  return process.env.MONGO_DB || "test";
}

function parseYYYYMM(s) {
  if (!/^\d{4}-\d{2}$/.test(s)) throw new Error(`Formato inválido: ${s}. Use YYYY-MM`);
  const [y, m] = s.split("-").map(Number);
  if (m < 1 || m > 12) throw new Error("Mês inválido.");
  return { y, m };
}

function monthRangeBR(yyyymm) {
  const { y, m } = parseYYYYMM(yyyymm);
  const startBR = `${String(y).padStart(4, "0")}-${String(m).padStart(2, "0")}-01T00:00:00`;
  const ny = m === 12 ? y + 1 : y;
  const nm = m === 12 ? 1 : m + 1;
  const endBR = `${String(ny).padStart(4, "0")}-${String(nm).padStart(2, "0")}-01T00:00:00`;
  return { startBR, endBR, y, m };
}

function oidToString(x) {
  if (!x) return null;
  if (typeof x === "string") return x;
  if (typeof x.toString === "function") return x.toString();
  return String(x);
}

function normalizeAccel(doc) {
  return {
    ts_raw: doc.ts,               // Date (UTC) do Mongo
    ts_br_raw: doc.ts_br ?? null, // string BR "YYYY-MM-DDTHH:mm:ss.SSS"
    company_id: oidToString(doc?.meta?.company_id),
    bridge_id: oidToString(doc?.meta?.bridge_id),
    device_id: doc.device_id ?? doc?.meta?.device_id ?? null,
    axis: doc?.meta?.axis ?? null,
    severity: doc.severity ?? null,
    value: doc.value ?? null,
  };
}

function escapeWin(p) {
  return p.replaceAll("\\", "\\\\");
}

async function connectWithRetry(client, tries = 3) {
  let lastErr;
  for (let i = 1; i <= tries; i++) {
    try {
      await client.connect();
      return;
    } catch (e) {
      lastErr = e;
      console.log(`Falhou conectar (tentativa ${i}/${tries}). Retry...`);
      await new Promise((r) => setTimeout(r, 2000 * i));
    }
  }
  throw lastErr;
}

async function main() {
  const yyyymm = process.argv[2];
  if (!yyyymm) {
    console.error("Uso: node ETL/export_accel_month_br.js YYYY-MM  (ex: 2025-11)");
    process.exit(1);
  }

  const MONGO_URI = mustEnv("MONGO_URI");
  const DUCKDB_CLI = mustEnv("DUCKDB_CLI");
  const dbName = resolveDbNameFromUri(MONGO_URI);

  const { startBR, endBR, y, m } = monthRangeBR(yyyymm);

  // output: ETL/tmp/telemetry_accel/raw/YYYY/MM/accel_YYYY-MM.parquet
  const outDir = path.resolve("ETL/tmp/telemetry_accel/raw", String(y), String(m).padStart(2, "0"));
  fs.mkdirSync(outDir, { recursive: true });
  const outParquet = path.join(outDir, `accel_${yyyymm}.parquet`);

  // NDJSON temp
  const tmpBase = fs.mkdtempSync(path.join(os.tmpdir(), "d2win-etl-"));
  const ndjsonPath = path.join(tmpBase, `accel_${yyyymm}.ndjson`);

  console.log("DB selecionado:", dbName);
  console.log("Mês (BR):", yyyymm);
  console.log("Faixa ts_br:", startBR, "->", endBR);
  console.log("[1/3] Buscando Mongo (ts_br)...");

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

    // filtro BR: usa string ts_br
    const cursor = col.find(
      { ts_br: { $gte: startBR, $lt: endBR } },
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

  console.log("[2/3] Gerando Parquet com DuckDB (ts_br_ts como base BR)...");

  const nd = escapeWin(ndjsonPath);
  const out = escapeWin(outParquet);

  const sql = `
CREATE OR REPLACE TABLE t AS
SELECT
  -- guardamos ts_utc também, mas BR é a referência
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
FROM read_json_auto('${nd}')
WHERE ts_br_raw IS NOT NULL;

COPY t TO '${out}' (FORMAT PARQUET);
`;

  const r = spawnSync(DUCKDB_CLI, [":memory:", "-c", sql], { encoding: "utf-8" });
  if (r.status !== 0) throw new Error(`DuckDB falhou:\n${r.stderr || r.stdout}`);

  console.log("[3/3] OK ✅ Parquet criado em:", outParquet);
}

main().catch((e) => {
  console.error("Erro:", e);
  process.exit(1);
});