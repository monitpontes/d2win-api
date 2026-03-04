// src/scripts/backfillDaily.js
import "dotenv/config";
import mongoose from "mongoose";

import { rollupDailyAccel } from "../services/rollup/dailyAccel.js";
import { rollupDailyFreq } from "../services/rollup/dailyFreq.js";

const BR_OFFSET_MS = 3 * 60 * 60 * 1000;

function startOfDayBrUtcFromBrIso(yyyyMmDd) {
  return new Date(`${yyyyMmDd}T00:00:00-03:00`);
}

function lastCompleteDayEndUtc(now = new Date()) {
  const nowBr = new Date(now.getTime() - BR_OFFSET_MS);
  nowBr.setHours(0, 0, 0, 0);
  return new Date(nowBr.getTime() + BR_OFFSET_MS);
}

async function getMinBucket(db, collName) {
  const doc = await db
    .collection(collName)
    .find({}, { projection: { bucket_br: 1 } })
    .sort({ bucket_br: 1 })
    .limit(1)
    .next();
  return doc?.bucket_br ? new Date(doc.bucket_br) : null;
}

function* dayWindows(fromUtc, toUtc) {
  let t = new Date(fromUtc);
  while (t < toUtc) {
    const next = new Date(t.getTime() + 86400_000);
    yield { fromUtc: new Date(t), toUtc: next };
    t = next;
  }
}

async function main() {
  const uri = process.env.MONGO_URI;
  if (!uri) throw new Error("MONGO_URI ausente no .env");

  await mongoose.connect(uri);
  const db = mongoose.connection.db;

  const fromEnv = (process.env.DAILY_BACKFILL_FROM || "").trim();

  let fromUtc;
  if (fromEnv) {
    fromUtc = startOfDayBrUtcFromBrIso(fromEnv);
  } else {
    const minA = await getMinBucket(db, "telemetry_rollup_hourly_accel");
    const minF = await getMinBucket(db, "telemetry_rollup_hourly_freq");
    const mins = [minA, minF].filter(Boolean);

    if (!mins.length) {
      console.log("[backfillDaily] Não achei hourly pra iniciar. Defina DAILY_BACKFILL_FROM.");
      process.exit(0);
    }

    // arredonda pro início do dia BR
    const minUtc = new Date(Math.min(...mins.map((d) => d.getTime())));
    const minBr = new Date(minUtc.getTime() - BR_OFFSET_MS);
    minBr.setHours(0, 0, 0, 0);
    fromUtc = new Date(minBr.getTime() + BR_OFFSET_MS);
  }

  const endUtc = lastCompleteDayEndUtc(new Date()); // fecha só dias completos (até ontem)

  const days = Math.floor((endUtc.getTime() - fromUtc.getTime()) / 86400_000);
  console.log("[backfillDaily] range", {
    fromUtc: fromUtc.toISOString(),
    toUtc: endUtc.toISOString(),
    days,
  });

  let n = 0;
  for (const w of dayWindows(fromUtc, endUtc)) {
    console.log("[backfillDaily] closing", w.fromUtc.toISOString(), "->", w.toUtc.toISOString());
    await rollupDailyAccel(db, w);
    await rollupDailyFreq(db, w);
    n++;
    if (n % 7 === 0) console.log(`[backfillDaily] progress: ${n} dias`);
  }

  console.log(`[backfillDaily] DONE. Days closed: ${n}`);
  await mongoose.disconnect();
}

main().catch((e) => {
  console.error("[backfillDaily] ERROR", e?.message || e);
  process.exit(1);
});