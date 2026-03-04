// src/scripts/backfillHourly.js
import "dotenv/config";
import mongoose from "mongoose";
import { rollupHourlyAccel } from "../services/rollup/hourlyAccel.js";
import { rollupHourlyFreq } from "../services/rollup/hourlyFreq.js";

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} ausente no .env`);
  return v;
}

function floorToHourUtc(d) {
  const x = new Date(d);
  x.setUTCMinutes(0, 0, 0);
  return x;
}

function* hourWindows(fromUtc, toUtc) {
  let t = new Date(fromUtc);
  while (t < toUtc) {
    const next = new Date(t.getTime() + 3600_000);
    yield { fromUtc: new Date(t), toUtc: next };
    t = next;
  }
}

async function minTs(db, coll) {
  const doc = await db.collection(coll)
    .find({}, { projection: { ts: 1 } })
    .sort({ ts: 1 })
    .limit(1)
    .next();
  return doc?.ts ? new Date(doc.ts) : null;
}

async function run() {
  const uri = mustEnv("MONGO_URI");
  await mongoose.connect(uri);
  const db = mongoose.connection.db;

  const RAW_ACCEL = "telemetry_ts_accel";
  const RAW_FREQ  = "telemetry_ts_freq_peaks";

  const a = await minTs(db, RAW_ACCEL);
  const f = await minTs(db, RAW_FREQ);

  const mins = [a, f].filter(Boolean);
  if (!mins.length) {
    console.log("[backfillHourly] nenhuma amostra em RAW accel/freq");
    process.exit(0);
  }

  const fromUtc = floorToHourUtc(new Date(Math.min(...mins.map(d => d.getTime()))));
  const toUtc   = floorToHourUtc(new Date()); // não pega a hora corrente incompleta

  const totalHours = Math.floor((toUtc.getTime() - fromUtc.getTime()) / 3600_000);
  console.log("[backfillHourly] range", {
    fromUtc: fromUtc.toISOString(),
    toUtc: toUtc.toISOString(),
    totalHours
  });

  let i = 0;
  for (const w of hourWindows(fromUtc, toUtc)) {
    i++;
    const rA = await rollupHourlyAccel(db, w);
    const rF = await rollupHourlyFreq(db, w);

    if (i % 24 === 0 || i === 1 || i === totalHours) {
      console.log(`[backfillHourly] ${i}/${totalHours} hour=${w.fromUtc.toISOString()} accel=${rA?.ms}ms freq=${rF?.ms}ms`);
    }
  }

  console.log("[backfillHourly] DONE");
  process.exit(0);
}

run().catch((e) => {
  console.error("[backfillHourly] ERROR:", e?.message || e);
  process.exit(1);
});