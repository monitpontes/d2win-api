// src/lib/s3Telemetry.js
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { spawnSync } from "node:child_process";
import { getObjectBuffer, listPrefix, existsObject } from "../services/s3Objects.js";

/* =========================
   Timezone BR (fixo UTC-03)
   ========================= */

const BR_OFFSET_MS = 3 * 60 * 60 * 1000;

function toDuckTsUTC(d) {
  // YYYY-MM-DD HH:MM:SS
  return d.toISOString().replace("T", " ").slice(0, 19);
}

/**
 * Input do usuário vem como "YYYY-MM-DD" e o parseISODate vira Date em UTC 00:00.
 * Mas "dia BR" começa em 00:00 BR = 03:00 UTC, então:
 * ✅ para filtrar em "ts"/"ts_utc" (UTC), somamos +3h no from/to.
 */
export function toDuckTsUTCFromBrInput(d) {
  return toDuckTsUTC(new Date(d.getTime() + BR_OFFSET_MS));
}

/* =========================
   Helpers
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

/**
 * MonthsBetween em UTC (não depende do timezone do Windows)
 * Retorna [{year, month}] incluindo endpoints.
 */
export function monthsBetween(fromDate, toDate) {
  const out = [];

  const start = new Date(Date.UTC(fromDate.getUTCFullYear(), fromDate.getUTCMonth(), 1));
  const end = new Date(Date.UTC(toDate.getUTCFullYear(), toDate.getUTCMonth(), 1));

  let y = start.getUTCFullYear();
  let m = start.getUTCMonth();

  while (y < end.getUTCFullYear() || (y === end.getUTCFullYear() && m <= end.getUTCMonth())) {
    out.push({ year: y, month: m + 1 });
    m++;
    if (m > 11) {
      m = 0;
      y++;
    }
  }

  return out;
}

/**
 * "YYYY-MM-DD" (dia BR) => Date UTC do início do dia BR
 * 00:00 BR == 03:00 UTC
 */
function brDayStartUtc(dateStr) {
  return new Date(`${dateStr}T03:00:00.000Z`);
}

/* =========================
   S3 keys - LEGADO (mensal)
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

export function keysForAggRangeLegacy({ domain, granularity, from, to }) {
  const months = monthsBetween(from, to);
  return months.map(({ year, month }) => buildAggKey({ domain, granularity, year, month }));
}

export function keysForRawRangeLegacy({ domain, from, to }) {
  const months = monthsBetween(from, to);
  return months.map(({ year, month }) => buildRawKey({ domain, year, month }));
}

/* =========================
   S3 keys - NOVO (diário)
   ========================= */

function dailyRegexFor({ domain, source, granularity }) {
  const dom = domain === "accel" ? "accel" : "freq";

  if (source === "raw") {
    // telemetry_freq/raw/2026/03/freq_raw_2026-03-01.parquet
    return new RegExp(
      `^telemetry_${dom}\\/raw\\/\\d{4}\\/\\d{2}\\/${dom}_raw_(\\d{4}-\\d{2}-\\d{2})\\.parquet$`
    );
  }

  // telemetry_freq/agg/2026/03/hourly/freq_hourly_2026-03-01.parquet
  return new RegExp(
    `^telemetry_${dom}\\/agg\\/\\d{4}\\/\\d{2}\\/${granularity}\\/${dom}_${granularity}_(\\d{4}-\\d{2}-\\d{2})\\.parquet$`
  );
}

async function listDailyKeysForRange({ domain, source, granularity, from, to }) {
  const months = monthsBetween(from, to);
  const rx = dailyRegexFor({ domain, source, granularity });
  const out = [];

  for (const { year, month } of months) {
    const yyyy = String(year);
    const mm = pad2(month);

    const prefix =
      source === "raw"
        ? `telemetry_${domain}/raw/${yyyy}/${mm}/`
        : `telemetry_${domain}/agg/${yyyy}/${mm}/${granularity}/`;

    const objs = await listPrefix(prefix);

    for (const o of objs) {
      const key = o.key;
      const m = rx.exec(key);
      if (!m) continue;

      const dateStr = m[1]; // YYYY-MM-DD (dia BR)
      const dayStart = brDayStartUtc(dateStr);
      const dayEnd = new Date(dayStart.getTime() + 24 * 60 * 60 * 1000);

      // inclui se o arquivo diário intersecta [from,to)
      if (dayEnd <= from) continue;
      if (dayStart >= to) continue;

      out.push(key);
    }
  }

  out.sort();
  return out;
}

/* =========================
   AUTO resolver (mistura daily + legacy)
   - Nunca inclui key inexistente
   - Se legacy existe no mês e daily não cobre o "pedaço" do mês => usa legacy
   - Se legacy não existe no mês (ex.: mês novo) => usa daily mesmo parcial
   ========================= */

function monthId(year, month) {
  return `${year}-${pad2(month)}`;
}

function dayFromDailyKey(key) {
  const m = key.match(/_(\d{4}-\d{2}-\d{2})\.parquet$/);
  return m ? m[1] : null;
}

function expectedDaysForMonthSlice(from, to, year, month) {
  // from/to são Date (UTC midnight do input)
  const startMonth = new Date(Date.UTC(year, month - 1, 1));
  const endMonth = new Date(Date.UTC(year, month, 1)); // próximo mês

  const sliceStart = from > startMonth ? from : startMonth;
  const sliceEnd = to < endMonth ? to : endMonth;

  const set = new Set();
  const cur = new Date(Date.UTC(sliceStart.getUTCFullYear(), sliceStart.getUTCMonth(), sliceStart.getUTCDate()));
  const end = new Date(Date.UTC(sliceEnd.getUTCFullYear(), sliceEnd.getUTCMonth(), sliceEnd.getUTCDate()));

  while (cur < end) {
    const y = cur.getUTCFullYear();
    const m = pad2(cur.getUTCMonth() + 1);
    const d = pad2(cur.getUTCDate());
    set.add(`${y}-${m}-${d}`);
    cur.setUTCDate(cur.getUTCDate() + 1);
  }

  return set;
}

function dailyCoversExpected(dailyKeys, expectedSet) {
  if (!expectedSet.size) return true;
  const got = new Set();
  for (const k of dailyKeys) {
    const day = dayFromDailyKey(k);
    if (day) got.add(day);
  }
  for (const d of expectedSet) {
    if (!got.has(d)) return false;
  }
  return true;
}

async function existsMany(keys) {
  // cache simples pra não ficar chamando existsObject repetido
  const cache = new Map();
  const out = [];
  for (const k of keys) {
    if (!cache.has(k)) cache.set(k, await existsObject(k));
    if (cache.get(k)) out.push(k);
  }
  return out;
}

export async function keysForRawRangeAuto({ domain, from, to }) {
  const months = monthsBetween(from, to);

  // daily candidatos (podem ser parciais)
  const dailyAll = await listDailyKeysForRange({ domain, source: "raw", from, to });
  const dailyByMonth = new Map();
  for (const k of dailyAll) {
    const m = k.match(/\/(\d{4})\/(\d{2})\//);
    if (!m) continue;
    const id = `${m[1]}-${m[2]}`;
    if (!dailyByMonth.has(id)) dailyByMonth.set(id, []);
    dailyByMonth.get(id).push(k);
  }

  // legacy candidatos (filtra só os que existem)
  const legacyCandidates = months.map(({ year, month }) => buildRawKey({ domain, year, month }));
  const legacyExisting = new Set(await existsMany(legacyCandidates));

  const finalKeys = [];

  for (const { year, month } of months) {
    const id = monthId(year, month);
    const dailyKeys = (dailyByMonth.get(id) || []).slice().sort();
    const legacyKey = buildRawKey({ domain, year, month });
    const hasLegacy = legacyExisting.has(legacyKey);

    const expected = expectedDaysForMonthSlice(from, to, year, month);
    const dailyCovers = dailyCoversExpected(dailyKeys, expected);

    if (hasLegacy && !dailyCovers) {
      // ✅ legacy cobre esse pedaço do mês (e evita missing days)
      finalKeys.push(legacyKey);
    } else if (dailyKeys.length) {
      // ✅ usa daily
      finalKeys.push(...dailyKeys);
    } else if (hasLegacy) {
      finalKeys.push(legacyKey);
    }
  }

  // uniq + ordena
  return Array.from(new Set(finalKeys)).sort();
}

export async function keysForAggRangeAuto({ domain, granularity, from, to }) {
  if (granularity === "monthly") {
    // monthly é legado por definição, mas mesmo assim filtramos existência
    const legacy = keysForAggRangeLegacy({ domain, granularity, from, to });
    return await existsMany(legacy);
  }

  const months = monthsBetween(from, to);

  const dailyAll = await listDailyKeysForRange({ domain, source: "agg", granularity, from, to });
  const dailyByMonth = new Map();
  for (const k of dailyAll) {
    const m = k.match(/\/(\d{4})\/(\d{2})\//);
    if (!m) continue;
    const id = `${m[1]}-${m[2]}`;
    if (!dailyByMonth.has(id)) dailyByMonth.set(id, []);
    dailyByMonth.get(id).push(k);
  }

  const legacyCandidates = months.map(({ year, month }) => buildAggKey({ domain, granularity, year, month }));
  const legacyExisting = new Set(await existsMany(legacyCandidates));

  const finalKeys = [];

  for (const { year, month } of months) {
    const id = monthId(year, month);
    const dailyKeys = (dailyByMonth.get(id) || []).slice().sort();
    const legacyKey = buildAggKey({ domain, granularity, year, month });
    const hasLegacy = legacyExisting.has(legacyKey);

    const expected = expectedDaysForMonthSlice(from, to, year, month);
    const dailyCovers = dailyCoversExpected(dailyKeys, expected);

    if (hasLegacy && !dailyCovers) {
      finalKeys.push(legacyKey);
    } else if (dailyKeys.length) {
      finalKeys.push(...dailyKeys);
    } else if (hasLegacy) {
      finalKeys.push(legacyKey);
    }
  }

  return Array.from(new Set(finalKeys)).sort();
}

/* =========================
   DuckDB runner
   ========================= */

function writeTmpFile(buffer, filename, prefix = "d2win") {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `${prefix}-`));
  const out = path.join(dir, filename);
  fs.writeFileSync(out, buffer);
  return out;
}

function escPath(p) {
  // DuckDB aceita / no Windows; também escapa aspas simples
  return String(p).replaceAll("\\", "/").replaceAll("'", "''");
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

  // ✅ alinha colunas por nome (colunas ausentes viram NULL)
  return `CREATE OR REPLACE VIEW v AS
${parts.join("\nUNION ALL BY NAME\n")};`;
}

export function buildWhere(filters) {
  const clauses = [];
  for (const [col, value] of Object.entries(filters)) {
    if (!value) continue;
    clauses.push(`${col}='${value}'`);
  }
  return clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
}

export function buildWhereWithTime({ filters, timeCol, fromExpr, toExpr }) {
  // fromExpr/toExpr já vêm prontos (TIMESTAMP ... ou TIMESTAMPTZ ...)
  const clauses = [];
  for (const [col, value] of Object.entries(filters)) {
    if (!value) continue;
    clauses.push(`${col}='${value}'`);
  }
  if (fromExpr) clauses.push(`${timeCol} >= ${fromExpr}`);
  if (toExpr) clauses.push(`${timeCol} <  ${toExpr}`);
  return clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
}

export async function duckdbQueryJsonFromS3Keys({ keys, tmpPrefix, sqlSelect, threads = 4, debug = false }) {
  const duckdbCli = mustEnv("DUCKDB_CLI");
  if (!Array.isArray(keys) || !keys.length) return [];

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

  if (debug) {
    console.log("DUCKDB SQL PREVIEW:\n");
    console.log(sql.slice(0, 1400));
  }

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

export async function duckdbSchemaFromS3Keys({ keys, tmpPrefix, threads = 2 }) {
  const rows = await duckdbQueryJsonFromS3Keys({
    keys,
    tmpPrefix,
    threads,
    // ✅ sem ';' aqui
    sqlSelect: `SELECT name, type FROM pragma_table_info('v') ORDER BY cid`,
  });
  return rows || [];
}