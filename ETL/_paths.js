// ETL/_paths.js
import path from "node:path";
import fs from "node:fs";

export function parseYYYYMM(yyyymm) {
  if (!/^\d{4}-\d{2}$/.test(yyyymm)) {
    throw new Error(`Formato inválido. Use YYYY-MM (ex: 2026-02). Recebido: ${yyyymm}`);
  }
  const [y, m] = yyyymm.split("-").map(Number);
  if (m < 1 || m > 12) throw new Error("Mês inválido.");
  const yyyy = String(y).padStart(4, "0");
  const mm = String(m).padStart(2, "0");
  return { yyyy, mm };
}

export function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

export function escWin(p) {
  // para DuckDB CLI no Windows
  return p.replaceAll("\\", "\\\\");
}

export function tmpBase() {
  return path.resolve("ETL", "tmp");
}

export function datasetBase(dataset) {
  // dataset: "telemetry_accel" | "telemetry_freq"
  return path.join(tmpBase(), dataset);
}

export function rawMonthPath(dataset, yyyymm) {
  const { yyyy, mm } = parseYYYYMM(yyyymm);
  const base = datasetBase(dataset);
  const dir = path.join(base, "raw", yyyy, mm);
  ensureDir(dir);

  const prefix = dataset === "telemetry_accel" ? "accel" : "freq";
  const file = `${prefix}_${yyyymm}.parquet`;
  return path.join(dir, file);
}

export function aggMonthDir(dataset, yyyymm) {
  const { yyyy, mm } = parseYYYYMM(yyyymm);
  const base = datasetBase(dataset);
  const dir = path.join(base, "agg", yyyy, mm);
  ensureDir(dir);
  return dir;
}

export function aggOutPath(dataset, yyyymm, level) {
  // level: "hourly" | "daily" | "monthly"
  const dir = path.join(aggMonthDir(dataset, yyyymm), level);
  ensureDir(dir);

  const prefix = dataset === "telemetry_accel" ? "accel" : "freq";
  const file = `${prefix}_${level}_${yyyymm}.parquet`;
  return path.join(dir, file);
}