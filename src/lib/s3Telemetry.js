// src/lib/s3Telemetry.js
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { spawnSync } from "node:child_process";
import { getObjectBuffer } from "../services/s3Objects.js";

/* =========================
   Constantes
   ========================= */

export const RAW_TIME_COL = "ts_br_ts";   // RAW accel/freq
export const AGG_TIME_COL = "bucket_br";  // AGG accel/freq

// UTC-03 fixo
const BR_OFFSET_MS = 3 * 60 * 60 * 1000;

/* =========================
   Helpers básicos
   ========================= */

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} ausente no .env`);
  return v;
}
function pad2(n) {
  return String(n).padStart(2, "0");
}

export function mustOneOf(v, allowed, name) {
  if (!allowed.includes(v)) throw new Error(`${name} inválido. Use: ${allowed.join(", ")}`);
  return v;
}

export function safeToken(v, fieldName) {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s) return null;
  if (!/^[A-Za-z0-9._:-]+$/.test(s)) throw new Error(`Valor inválido em ${fieldName}`);
  return s;
}

export function parseISODate(v, field) {
  if (!v) throw new Error(`${field} é obrigatório`);
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) throw new Error(`${field} inválido: ${v}`);
  return d;
}

function toDuckTsUTC(d) {
  return d.toISOString().replace("T", " ").slice(0, 19); // YYYY-MM-DD HH:MM:SS
}

// Converte Date (interno UTC) para string de timestamp BR (UTC-03)
export function toDuckTsBR(d) {
  const br = new Date(d.getTime() - BR_OFFSET_MS);
  return toDuckTsUTC(br);
}

export function monthsBetween(fromDate, toDate) {
  const out = [];
  const start = new Date(fromDate.getFullYear(), fromDate.getMonth(), 1);
  const end = new Date(toDate.getFullYear(), toDate.getMonth(), 1);

  let y = start.getFullYear();
  let m = start.getMonth();
  while (y < end.getFullYear() || (y === end.getFullYear() && m <= end.getMonth())) {
    out.push({ year: y, month: m + 1 });
    m++;
    if (m > 11) { m = 0; y++; }
  }
  return out;
}

/* =========================
   Keys S3
   ========================= */

export function buildAggKey({ domain, granularity, year, month }) {
  const yyyy = String(year);
  const mm = pad2(month);
  const yyyymm = `${yyyy}-${mm}`;

  if (domain === "accel") {
    return `telemetry_accel/agg/${yyyy}/${mm}/${granularity}/accel_${granularity}_${yyyymm}.parquet`;
  }
  return `telemetry_freq/agg/${yyyy}/${mm}/${granularity}/freq_${granularity}_${yyyymm}.parquet`;
}

export function buildRawKey({ domain, year, month }) {
  const yyyy = String(year);
  const mm = pad2(month);
  const yyyymm = `${yyyy}-${mm}`;

  if (domain === "accel") {
    return `telemetry_accel/raw/${yyyy}/${mm}/accel_${yyyymm}.parquet`;
  }
  return `telemetry_freq/raw/${yyyy}/${mm}/freq_${yyyymm}.parquet`;
}

export function keysForAggRange({ domain, granularity, from, to }) {
  const months = monthsBetween(from, to);
  return months.map(({ year, month }) => buildAggKey({ domain, granularity, year, month }));
}

export function keysForRawRange({ domain, from, to }) {
  const months = monthsBetween(from, to);
  return months.map(({ year, month }) => buildRawKey({ domain, year, month }));
}

/* =========================
   DuckDB runner (multi parquet)
   ========================= */

function writeTmpFile(buffer, filename, prefix = "d2win") {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `${prefix}-`));
  const out = path.join(dir, filename);
  fs.writeFileSync(out, buffer);
  return out;
}
function escPath(p) {
  return p.replaceAll("\\", "\\\\");
}

async function downloadParquets(keys, prefix) {
  const locals = [];
  for (const key of keys) {
    const buf = await getObjectBuffer(key);
    locals.push(writeTmpFile(buf, path.basename(key), prefix));
  }
  return locals;
}

function buildUnionViewSql(localParquetPaths) {
  if (!localParquetPaths.length) throw new Error("Nenhum parquet para ler");
  const parts = localParquetPaths.map((p) => `SELECT * FROM read_parquet('${escPath(p)}')`);
  return `CREATE OR REPLACE VIEW v AS\n${parts.join("\nUNION ALL\n")};`;
}

export function buildWhere(filters) {
  const clauses = [];
  for (const [col, value] of Object.entries(filters)) {
    if (!value) continue;
    clauses.push(`${col}='${value}'`);
  }
  return clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
}

export function buildWhereWithTime({ filters, timeCol, fromTs, toTs }) {
  const clauses = [];
  for (const [col, value] of Object.entries(filters)) {
    if (!value) continue;
    clauses.push(`${col}='${value}'`);
  }
  if (fromTs) clauses.push(`${timeCol} >= TIMESTAMP '${fromTs}'`);
  if (toTs) clauses.push(`${timeCol} <  TIMESTAMP '${toTs}'`);
  return clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
}

export async function duckdbQueryJsonFromS3Keys({ keys, tmpPrefix, sqlSelect, threads = 4 }) {
  const duckdbCli = mustEnv("DUCKDB_CLI");

  const locals = await downloadParquets(keys, tmpPrefix);
  const preSql = buildUnionViewSql(locals);

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "d2win-duckdb-"));
  const outJsonPath = path.join(tmpDir, `out_${Date.now()}.json`);
  const outJsonEsc = escPath(outJsonPath);

  const sql = `
PRAGMA threads=${Number(threads) || 4};

${preSql}

COPY (
  ${sqlSelect}
) TO '${outJsonEsc}' (FORMAT JSON);
`;

  const r = spawnSync(duckdbCli, [":memory:", "-c", sql], {
    encoding: "utf-8",
    maxBuffer: 250 * 1024 * 1024,
  });

  if (r.status !== 0) {
    const msg = (r.stderr || r.stdout || "").trim();
    throw new Error(`DuckDB falhou:\n${msg || "sem detalhes"}`);
  }

  let text = "";
  try {
    if (!fs.existsSync(outJsonPath)) return [];
    text = fs.readFileSync(outJsonPath, "utf-8").trim();
  } finally {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
  }

  if (!text) return [];
  try {
    return JSON.parse(text);
  } catch {
    const lines = text.split("\n").map((l) => l.trim()).filter(Boolean);
    if (lines.length && lines[0].startsWith("{")) return lines.map((l) => JSON.parse(l));
    throw new Error(`JSON inválido do DuckDB. Primeiros 200 chars:\n${text.slice(0, 200)}`);
  }
}

/**
 * Schema (colunas + tipos) do view v.
 * Usa PRAGMA table_info('v') => retorna name/type etc.
 */
export async function duckdbSchemaFromS3Keys({ keys, tmpPrefix, threads = 2 }) {
  // PRAGMA table_info('v') retorna colunas:
  // cid, name, type, notnull, dflt_value, pk
  const rows = await duckdbQueryJsonFromS3Keys({
    keys,
    tmpPrefix,
    threads,
    sqlSelect: `SELECT name, type FROM pragma_table_info('v') ORDER BY cid;`,
  });
  return rows || [];
}