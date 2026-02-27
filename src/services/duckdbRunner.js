import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variável de ambiente ausente: ${name}`);
  return v;
}

function escWin(p) {
  return p.replaceAll("\\", "\\\\");
}

/**
 * Roda um SELECT e retorna JSON (array de rows) usando DuckDB CLI.
 * Ele escreve em arquivo temporário (compatível com Render, Linux e Windows).
 */
export function duckdbQueryToJson(selectSql) {
  const DUCKDB_CLI = mustEnv("DUCKDB_CLI"); // no Render: ./bin/duckdb, no Windows: duckdb

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "d2win-duckdb-"));
  const outJson = path.join(tmpDir, "out.json");

  // IMPORTANTE: DuckDB COPY JSON gera um JSON array (uma linha só).
  // Se você quiser NDJSON, dá pra trocar FORMAT JSON por FORMAT JSON; ARRAY true/false varia por versão,
  // então aqui vamos manter o padrão que funciona bem.
  const out = process.platform === "win32" ? escWin(outJson) : outJson;

  const sql = `
COPY (
  ${selectSql}
) TO '${out}' (FORMAT JSON);
`;

  const r = spawnSync(DUCKDB_CLI, [":memory:", "-c", sql], { encoding: "utf-8" });

  if (r.status !== 0) {
    throw new Error(`DuckDB falhou:\n${r.stderr || r.stdout || "sem detalhes"}`);
  }

  const raw = fs.readFileSync(outJson, "utf-8");
  // geralmente vem [] ou [{...}, {...}]
  const data = raw.trim() ? JSON.parse(raw) : [];

  // limpeza best-effort
  try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}

  return data;
}