// ETL/export_freq_month.js
// Export mensal (YYYY-MM) da collection telemetry_ts_freq_peaks usando filtro por ts (Date UTC).
// Gera: ETL/tmp/freq_YYYY-MM.parquet
//
// Inclui colunas derivadas:
// - f1, mag1, f2, mag2 (extraídos de doc.peaks[0..1] quando existirem)
// - f_max8/mag_max8/tag_max8 (maior freq >= 8 dentre f1/f2)
// - f_max9/mag_max9/tag_max9 (maior freq >= 9 dentre f1/f2)
// - mantém ts_raw, ts_br_raw, ts_utc (tipado), ts_br_ts (tipado)
//
// Uso: node ETL/export_freq_month.js 2026-02

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

function oidToString(x) {
  if (!x) return null;
  if (typeof x === "string") return x;
  if (typeof x.toString === "function") return x.toString();
  return String(x);
}

function pickPeak(peaks, idx) {
  if (!Array.isArray(peaks) || peaks.length <= idx) return { f: null, mag: null };
  const p = peaks[idx] || {};
  // tenta campos comuns
  const f = p.freq ?? p.frequency ?? p.f ?? null;
  const mag = p.mag ?? p.magnitude ?? p.amp ?? p.amplitude ?? null;
  return { f: f ?? null, mag: mag ?? null };
}

function maxOver(th, f1, mag1, f2, mag2) {
  // retorna {f, mag, tag} para maior f >= th
  let best = { f: null, mag: null, tag: null };
  const cands = [
    { f: f1, mag: mag1, tag: "f1" },
    { f: f2, mag: mag2, tag: "f2" },
  ].filter((x) => typeof x.f === "number" && isFinite(x.f));

  for (const c of cands) {
    if (c.f >= th) {
      if (best.f === null || c.f > best.f) best = c;
    }
  }
  return best;
}

function normalizeFreq(doc) {
  const peaks = doc.peaks ?? [];
  const p1 = pickPeak(peaks, 0);
  const p2 = pickPeak(peaks, 1);

  const f1 = typeof p1.f === "number" ? p1.f : null;
  const mag1 = typeof p1.mag === "number" ? p1.mag : null;
  const f2 = typeof p2.f === "number" ? p2.f : null;
  const mag2 = typeof p2.mag === "number" ? p2.mag : null;

  const max8 = maxOver(8, f1, mag1, f2, mag2);
  const max9 = maxOver(9, f1, mag1, f2, mag2);

  return {
    ts_raw: doc.ts,               // Date -> NDJSON vira ISO
    ts_br_raw: doc.ts_br ?? null, // string BR

    company_id: oidToString(doc?.meta?.company_id),
    bridge_id: oidToString(doc?.meta?.bridge_id),
    device_id: doc.device_id ?? doc?.meta?.device_id ?? null,

    stream: doc?.meta?.stream ?? null, // "freq:z"
    status: doc.status ?? null,
    severity: doc.severity ?? null,

    n: doc.n ?? null,
    fs: doc.fs ?? null,

    f1,
    mag1,
    f2,
    mag2,

    f_max8: max8.f,
    mag_max8: max8.mag,
    tag_max8: max8.tag,

    f_max9: max9.f,
    mag_max9: max9.mag,
    tag_max9: max9.tag,
  };
}

function escapeWin(p) {
  return p.replaceAll("\\", "\\\\");
}

async function main() {
  const yyyymm = process.argv[2];
  if (!yyyymm) {
    console.error("Uso: node ETL/export_freq_month.js YYYY-MM   (ex: 2026-02)");
    process.exit(1);
  }
  parseYYYYMM(yyyymm);

  const MONGO_URI = mustEnv("MONGO_URI");
  const DUCKDB_CLI = mustEnv("DUCKDB_CLI");
  const dbName = resolveDbNameFromUri(MONGO_URI);

  const start = monthStartUTC(yyyymm);
  const end = monthEndUTC(yyyymm);

  const tmpDir = path.resolve("ETL/tmp");
  fs.mkdirSync(tmpDir, { recursive: true });

  const tmpBase = fs.mkdtempSync(path.join(os.tmpdir(), "d2win-etl-"));
  const ndjsonPath = path.join(tmpBase, `freq_${yyyymm}.ndjson`);
  const outParquet = path.join(tmpDir, `freq_${yyyymm}.parquet`);

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
    const col = db.collection("telemetry_ts_freq_peaks");

    const cursor = col.find(
      { ts: { $gte: start, $lt: end } },
      { projection: { _id: 0 }, batchSize: 5000 }
    );

    const fd = fs.openSync(ndjsonPath, "w");
    try {
      for await (const doc of cursor) {
        fs.writeSync(fd, JSON.stringify(normalizeFreq(doc)) + "\n");
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
    console.log("Nada a exportar nesse mês.");
    return;
  }

  console.log("[2/3] Gerando Parquet com DuckDB (tipando timestamps)...");

  const nd = escapeWin(ndjsonPath);
  const out = escapeWin(outParquet);

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

  f1, mag1,
  f2, mag2,

  f_max8, mag_max8, tag_max8,
  f_max9, mag_max9, tag_max9
FROM read_json_auto('${nd}');

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