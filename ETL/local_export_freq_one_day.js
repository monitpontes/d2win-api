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

// peaks pode ser [] ou array de objetos.
// A gente salva como JSON string para ser estável no parquet.
function normalizeFreq(doc) {
  return {
    ts_raw: doc.ts,                 // vem Date do Mongo
    ts_br_raw: doc.ts_br ?? null,   // string BR

    company_id: oidToString(doc?.meta?.company_id),
    bridge_id: oidToString(doc?.meta?.bridge_id),
    device_id: doc.device_id ?? doc?.meta?.device_id ?? null,

    stream: doc?.meta?.stream ?? null, // ex: "freq:z"
    status: doc.status ?? null,
    severity: doc.severity ?? null,

    n: doc.n ?? null,
    fs: doc.fs ?? null,

    peaks_json: JSON.stringify(doc.peaks ?? []),
  };
}

async function main() {
  const isoDay = process.argv[2];
  if (!isoDay) {
    console.error("Uso: node ETL/local_export_freq_one_day.js YYYY-MM-DD");
    process.exit(1);
  }

  const MONGO_URI = mustEnv("MONGO_URI");
  const DUCKDB_CLI = mustEnv("DUCKDB_CLI"); // duckdb
  const dbName = resolveDbNameFromUri(MONGO_URI);

  const start = dayStartUTC(isoDay);
  const end = dayEndUTC(isoDay);

  // outputs
  const tmpDir = path.resolve("ETL/tmp");
  fs.mkdirSync(tmpDir, { recursive: true });

  const ndjsonPath = path.join(
    fs.mkdtempSync(path.join(os.tmpdir(), "d2win-etl-")),
    `freq_${isoDay}.ndjson`
  );

  const outParquet = path.join(tmpDir, `freq_${isoDay}.parquet`);

  console.log("DB selecionado:", dbName);
  console.log("[1/3] Buscando Mongo:", isoDay);

  const client = new MongoClient(MONGO_URI);
  await client.connect();

  let count = 0;
  try {
    const db = client.db(dbName);
    const col = db.collection("telemetry_ts_freq_peaks");

    const cursor = col.find(
      { ts: { $gte: start, $lt: end } },
      { projection: { _id: 0 }, batchSize: 2000 }
    );

    const fd = fs.openSync(ndjsonPath, "w");
    try {
      for await (const doc of cursor) {
        const row = normalizeFreq(doc);
        fs.writeSync(fd, JSON.stringify(row) + "\n");
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
    return;
  }

  console.log("[2/3] Gerando Parquet com DuckDB (tipando timestamps)...");

  // escape de paths no Windows
  const nd = ndjsonPath.replaceAll("\\", "\\\\");
  const out = outParquet.replaceAll("\\", "\\\\");

  // Aqui tipamos:
  // - ts_utc: TIMESTAMPTZ (UTC)
  // - ts_br_ts: TIMESTAMP (horário BR sem fuso, mas serve p/ filtros e buckets 0-23)
  //
  // ts_raw vem como Date no NDJSON -> geralmente vira string ISO; então fazemos cast via VARCHAR.
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
  stream,
  status,
  severity,
  n,
  fs,
  peaks_json
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