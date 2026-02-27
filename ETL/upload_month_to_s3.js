// ETL/upload_month_to_s3.js
// Upload por mês (YYYY-MM) para S3, mantendo o mesmo layout de pastas do ETL/tmp.
//
// Sobe (se existirem):
//   ETL/tmp/telemetry_accel/raw/YYYY/MM/**
//   ETL/tmp/telemetry_accel/agg/YYYY/MM/**
//   ETL/tmp/telemetry_freq/raw/YYYY/MM/**
//   ETL/tmp/telemetry_freq/agg/YYYY/MM/**
//
// Para o bucket com as chaves:
//   telemetry_accel/raw/YYYY/MM/**
//   telemetry_accel/agg/YYYY/MM/**
//   telemetry_freq/raw/YYYY/MM/**
//   telemetry_freq/agg/YYYY/MM/**
//
// Uso:
//   node ETL/upload_month_to_s3.js 2025-10
//   node ETL/upload_month_to_s3.js 2025-10 accel
//   node ETL/upload_month_to_s3.js 2025-10 freq
//   node ETL/upload_month_to_s3.js 2025-10 all

import dotenv from "dotenv";
import path from "node:path";
import fs from "node:fs";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

dotenv.config();

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variável de ambiente ausente: ${name}`);
  return v;
}

function parseYYYYMM(s) {
  if (!/^\d{4}-\d{2}$/.test(s)) throw new Error(`Use YYYY-MM (recebido: ${s})`);
  const [y, m] = s.split("-").map(Number);
  if (m < 1 || m > 12) throw new Error(`Mês inválido: ${m}`);
  return { y, m, yyyy: String(y), mm: String(m).padStart(2, "0") };
}

function walkFiles(dir) {
  const out = [];
  if (!fs.existsSync(dir)) return out;

  const stack = [dir];
  while (stack.length) {
    const cur = stack.pop();
    const entries = fs.readdirSync(cur, { withFileTypes: true });
    for (const e of entries) {
      const full = path.join(cur, e.name);
      if (e.isDirectory()) stack.push(full);
      else if (e.isFile()) out.push(full);
    }
  }
  return out;
}

function mimeFor(filePath) {
  if (filePath.endsWith(".parquet")) return "application/octet-stream";
  if (filePath.endsWith(".json")) return "application/json";
  if (filePath.endsWith(".ndjson")) return "application/x-ndjson";
  return "application/octet-stream";
}

async function uploadOne(s3, bucket, key, filePath) {
  const body = fs.createReadStream(filePath);
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: mimeFor(filePath),
    })
  );
}

async function uploadFolderToPrefix({ s3, bucket, localDir, keyPrefix }) {
  const files = walkFiles(localDir);
  if (files.length === 0) {
    console.log("  (vazio) ", localDir);
    return { count: 0 };
  }

  console.log(`  ${files.length} arquivos: ${localDir}`);
  for (let i = 0; i < files.length; i++) {
    const filePath = files[i];
    const rel = path.relative(localDir, filePath).replaceAll("\\", "/");
    const key = `${keyPrefix}/${rel}`;

    process.stdout.write(`    [${i + 1}/${files.length}] ${key} ... `);
    await uploadOne(s3, bucket, key, filePath);
    process.stdout.write("OK\n");
  }
  return { count: files.length };
}

async function main() {
  const yyyymm = process.argv[2];
  const mode = (process.argv[3] || "all").toLowerCase(); // all | accel | freq

  if (!yyyymm) {
    console.error("Uso: node ETL/upload_month_to_s3.js YYYY-MM [all|accel|freq]");
    process.exit(1);
  }

  if (!["all", "accel", "freq"].includes(mode)) {
    console.error("Modo inválido. Use: all | accel | freq");
    process.exit(1);
  }

  const { yyyy, mm } = parseYYYYMM(yyyymm);

  const bucket = mustEnv("S3_BUCKET");
  const region = mustEnv("AWS_REGION");

  const s3 = new S3Client({ region });

  // Bases locais (mantendo seu layout)
  const baseLocal = path.resolve("ETL/tmp");

  const targets = [];
  if (mode === "all" || mode === "accel") {
    targets.push({
      name: "telemetry_accel/raw",
      localDir: path.join(baseLocal, "telemetry_accel", "raw", yyyy, mm),
      keyPrefix: `telemetry_accel/raw/${yyyy}/${mm}`,
    });
    targets.push({
      name: "telemetry_accel/agg",
      localDir: path.join(baseLocal, "telemetry_accel", "agg", yyyy, mm),
      keyPrefix: `telemetry_accel/agg/${yyyy}/${mm}`,
    });
  }
  if (mode === "all" || mode === "freq") {
    targets.push({
      name: "telemetry_freq/raw",
      localDir: path.join(baseLocal, "telemetry_freq", "raw", yyyy, mm),
      keyPrefix: `telemetry_freq/raw/${yyyy}/${mm}`,
    });
    targets.push({
      name: "telemetry_freq/agg",
      localDir: path.join(baseLocal, "telemetry_freq", "agg", yyyy, mm),
      keyPrefix: `telemetry_freq/agg/${yyyy}/${mm}`,
    });
  }

  console.log("📅 Mês:", yyyymm);
  console.log("🪣 Bucket:", bucket);
  console.log("🌎 Region:", region);
  console.log("🔎 Modo:", mode);
  console.log("");

  let total = 0;

  for (const t of targets) {
    console.log("➡️  Subindo:", t.name);
    if (!fs.existsSync(t.localDir)) {
      console.log("  (pasta não existe) ", t.localDir);
      continue;
    }
    const r = await uploadFolderToPrefix({
      s3,
      bucket,
      localDir: t.localDir,
      keyPrefix: t.keyPrefix,
    });
    total += r.count;
  }

  console.log(`\n✅ Upload do mês concluído. Arquivos enviados: ${total}`);
}

main().catch((e) => {
  console.error("Erro:", e);
  process.exit(1);
});