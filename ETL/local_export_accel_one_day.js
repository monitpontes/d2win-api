import dotenv from "dotenv";
import path from "node:path";
import fs from "node:fs";
import os from "node:os";
import { MongoClient } from "mongodb";
import { spawnSync } from "node:child_process";

dotenv.config(); // lê .env da raiz (rode sempre da raiz)

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

function dayStartUTC(isoDay) {
  const [y, m, d] = isoDay.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d, 0, 0, 0));
}
function dayEndUTC(isoDay) {
  const [y, m, d] = isoDay.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d + 1, 0, 0, 0));
}

function oidToString(x) {
  if (!x) return null;
  if (typeof x === "string") return x;
  if (typeof x.toString === "function") return x.toString();
  return String(x);
}

function normalizeAccel(doc) {
  return {
    ts_raw: doc.ts,                 // Date -> vira ISO no NDJSON
    ts_br_raw: doc.ts_br ?? null,   // string BR
    company_id: oidToString(doc?.meta?.company_id),
    bridge_id: oidToString(doc?.meta?.bridge_id),
    device_id: doc.device_id ?? doc?.meta?.device_id ?? null,
    axis: doc?.meta?.axis ?? null,
    severity: doc.severity ?? null,
    value: doc.value ?? null,
  };
}

async function exportAccelRange({ isoLabel, start, end }) {
  const MONGO_URI = mustEnv("MONGO_URI");
  const DUCKDB_CLI = mustEnv("DUCKDB_CLI");
  const dbName = resolveDbNameFromUri(MONGO_URI);

  const tmpDir = path.resolve("ETL/tmp");
  fs.mkdirSync(tmpDir, { recursive: true });

  const tmpBase = fs.mkdtempSync(path.join(os.tmpdir(), "d2win-etl-"));
  const ndjsonPath = path.join(tmpBase, `accel_${isoLabel}.ndjson`);
  const outParquet = path.join(tmpDir, `accel_${isoLabel}.parquet`);

  console.log("DB selecionado:", dbName);
  console.log("[1/3] Buscando Mongo:", isoLabel);

  const client = new MongoClient(MONGO_URI, {
    serverSelectionTimeoutMS: 10000,
    connectTimeoutMS: 10000,
  });

  let count = 0;
  await client.connect();
  try {
    const db = client.db(dbName);
    const col = db.collection("telemetry_ts_accel");

    const cursor = col.find(
      { ts: { $gte: start, $lt: end } },
      { projection: { _id: 0 }, batchSize: 2000 }
    );

    const fd = fs.openSync(ndjsonPath, "w");
    try {
      for await (const doc of cursor) {
        fs.writeSync(fd, JSON.stringify(normalizeAccel(doc)) + "\n");
        count++;
      }
    } finally {
      fs.closeSync(fd);
    }
  } finally {
    await client.close();
  }

  console.log("  docs:", count);
  if (count === 0) {
    console.log("Dia sem dados. Nada a exportar.");
    return null;
  }

  console.log("[2/3] Gerando Parquet com DuckDB (tipando timestamps)...");

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
  if (r.status !== 0) throw new Error(`DuckDB falhou:\n${r.stderr || r.stdout}`);

  console.log("[3/3] OK ✅ Parquet criado em:", outParquet);
  return outParquet;
}

async function main() {
  const isoDay = process.argv[2];
  if (!isoDay) {
    console.error("Uso: node ETL/local_export_accel_one_day.js YYYY-MM-DD");
    process.exit(1);
  }

  const start = dayStartUTC(isoDay);
  const end = dayEndUTC(isoDay);

  await exportAccelRange({ isoLabel: isoDay, start, end });
}

main().catch((e) => {
  console.error("Erro:", e);
  process.exit(1);
});