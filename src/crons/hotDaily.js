// src/crons/hotDaily.js
import cron from "node-cron";

import { rollupDailyAccel } from "../services/rollup/dailyAccel.js";
import { rollupDailyFreq } from "../services/rollup/dailyFreq.js";

const TZ_BR = "America/Sao_Paulo";
const BR_OFFSET_MS = 3 * 60 * 60 * 1000;

function lastCompleteDayEndUtc(now = new Date()) {
  // início de HOJE no BR, convertido pra UTC
  const nowBr = new Date(now.getTime() - BR_OFFSET_MS);
  nowBr.setHours(0, 0, 0, 0);
  return new Date(nowBr.getTime() + BR_OFFSET_MS);
}

function dayStartUtcFromEndUtc(endUtc) {
  return new Date(endUtc.getTime() - 86400_000);
}

async function getLastClosedDayEndUtc(db) {
  const colA = db.collection("telemetry_rollup_daily_accel");
  const colF = db.collection("telemetry_rollup_daily_freq");

  const lastA = await colA.find({}, { projection: { ts: 1 } }).sort({ ts: -1 }).limit(1).next();
  const lastF = await colF.find({}, { projection: { ts: 1 } }).sort({ ts: -1 }).limit(1).next();

  const tsA = lastA?.ts ? new Date(lastA.ts) : null;
  const tsF = lastF?.ts ? new Date(lastF.ts) : null;

  if (!tsA && !tsF) return null;
  if (tsA && tsF) return new Date(Math.max(tsA.getTime(), tsF.getTime()));
  return tsA || tsF;
}

async function catchUpDaily(db) {
  const lastEndUtc = await getLastClosedDayEndUtc(db);
  const targetEndUtc = lastCompleteDayEndUtc(new Date());

  if (!lastEndUtc) {
    console.log("[hotDaily] catch-up: daily vazio (sem ts). Use backfillDaily.js se precisar.");
    return;
  }

  let endUtc = new Date(lastEndUtc.getTime() + 86400_000);
  let n = 0;

  while (endUtc <= targetEndUtc) {
    const startUtc = dayStartUtcFromEndUtc(endUtc);
    console.log("[hotDaily] catch-up closing", startUtc.toISOString(), "->", endUtc.toISOString());

    await rollupDailyAccel(db, { fromUtc: startUtc, toUtc: endUtc });
    await rollupDailyFreq(db, { fromUtc: startUtc, toUtc: endUtc });

    n++;
    endUtc = new Date(endUtc.getTime() + 86400_000);
  }

  console.log(`[hotDaily] catch-up done. Days closed: ${n}`);
}

function secondsUntilNextRunBR() {
  const now = new Date();
  const todayStartUtc = lastCompleteDayEndUtc(now);
  const nextRunUtc = new Date(todayStartUtc.getTime() + 10_000); // 00:00:10 BR
  let diff = Math.floor((nextRunUtc.getTime() - now.getTime()) / 1000);
  if (diff < 0) diff += 86400;
  return diff;
}

export async function startHotDaily(db) {
  const enabled = (process.env.START_ROLLUPS || "true").toLowerCase() === "true";
  if (!enabled) {
    console.log("[hotDaily] disabled (START_ROLLUPS=false)");
    return;
  }

  // 1) fecha dias perdidos ao subir
  await catchUpDaily(db);

  console.log("[hotDaily] next run in", secondsUntilNextRunBR(), "seconds");

  // 2) roda todo dia 00:00:10 BR (6 campos: sec min hour day month weekday)
  cron.schedule(
    "10 0 0 * * *",
    async () => {
      try {
        const endUtc = lastCompleteDayEndUtc(new Date());  // início do dia atual BR em UTC
        const startUtc = dayStartUtcFromEndUtc(endUtc);    // dia anterior completo

        console.log("[hotDaily] closing day", startUtc.toISOString(), "->", endUtc.toISOString());

        const t0 = Date.now();
        const rA = await rollupDailyAccel(db, { fromUtc: startUtc, toUtc: endUtc });
        const accelMs = Date.now() - t0;

        const t1 = Date.now();
        const rF = await rollupDailyFreq(db, { fromUtc: startUtc, toUtc: endUtc });
        const freqMs = Date.now() - t1;

        console.log(`[hotDaily] closed day accel=${accelMs}ms freq=${freqMs}ms`);
        console.log("[hotDaily] next run in", secondsUntilNextRunBR(), "seconds");
      } catch (err) {
        console.error("[hotDaily] error", err?.message || err);
      }
    },
    { timezone: TZ_BR }
  );

  console.log("[hotDaily] started (cron BR 00:00:10 + catch-up on boot)");
}