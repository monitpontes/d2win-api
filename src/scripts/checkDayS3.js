// src/scripts/checkDayS3.js
import "dotenv/config";
import mongoose from "mongoose";
import { connectMongo } from "../lib/db.js";
import { existsObject } from "../services/s3Objects.js";

const TZ_BR = "America/Sao_Paulo";
const CONTROL_COLL = "export_jobs";

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} ausente no .env`);
  return v;
}

function isYYYYMMDD(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
}

function s3Key(domain, type, dayBr) {
  const [Y, M] = dayBr.split("-");
  if (type === "raw") {
    return `telemetry_${domain}/raw/${Y}/${M}/${domain}_raw_${dayBr}.parquet`;
  }
  return `telemetry_${domain}/agg/${Y}/${M}/${type}/${domain}_${type}_${dayBr}.parquet`;
}

function keysForDay(dayBr) {
  return [
    s3Key("accel", "raw", dayBr),
    s3Key("freq", "raw", dayBr),
    s3Key("accel", "hourly", dayBr),
    s3Key("freq", "hourly", dayBr),
    s3Key("accel", "daily", dayBr),
    s3Key("freq", "daily", dayBr),
  ];
}

async function checkKeys(dayBr) {
  const keys = keysForDay(dayBr);
  const pairs = await Promise.all(keys.map(async (k) => [k, await existsObject(k)]));
  const existsByKey = Object.fromEntries(pairs);
  const allExist = keys.every((k) => !!existsByKey[k]);
  return { keys, existsByKey, allExist };
}

async function markInMongo(db, dayBr, { allExist, existsByKey }) {
  const col = db.collection(CONTROL_COLL);

  await col.createIndex({ job: 1, day_br: 1 }, { unique: true, name: "uniq_export_job_day" });

  await col.updateOne(
    { job: "export_d5", day_br: dayBr },
    {
      $set: {
        job: "export_d5",
        day_br: dayBr,
        done: !!allExist,
        last_result: allExist ? "reconciled_from_s3" : "partial_missing_in_s3",
        s3_exists: existsByKey,
        updated_at: new Date(),
      },
      $setOnInsert: { created_at: new Date() },
    },
    { upsert: true }
  );
}

async function main() {
  const dayBr = process.argv[2];
  if (!isYYYYMMDD(dayBr)) {
    console.log("Uso: node src/scripts/checkDayS3.js YYYY-MM-DD");
    process.exit(1);
  }

  const bucket = mustEnv("S3_BUCKET");
  const mongoUri = mustEnv("MONGO_URI");

  console.log(`[check] dia BR: ${dayBr}`);
  console.log(`[check] bucket: ${bucket}`);

  // 1) checa S3
  const r = await checkKeys(dayBr);

  console.log("\n[check] S3 exists:");
  for (const k of r.keys) {
    console.log(` - ${r.existsByKey[k] ? "OK " : "MISS"} s3://${bucket}/${k}`);
  }
  console.log(`\n[check] allExist=${r.allExist}`);

  // 2) marca no Mongo (export_jobs)
  await connectMongo(mongoUri);
  const nativeDb = mongoose.connection.db;

  await markInMongo(nativeDb, dayBr, r);

  console.log(`[check] export_jobs atualizado: job=export_d5 day_br=${dayBr} done=${r.allExist}`);

  await mongoose.disconnect();
}

main().catch((e) => {
  console.error("[check] erro:", e?.message || e);
  process.exit(1);
});