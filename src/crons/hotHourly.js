import cron from "node-cron";
import mongoose from "mongoose";

import { rollupHourlyAccel } from "../services/rollup/hourlyAccel.js";
import { rollupHourlyFreq } from "../services/rollup/hourlyFreq.js";

const TZ_BR = "America/Sao_Paulo";
const BR_OFFSET_MS = 3 * 60 * 60 * 1000;

function lastCompleteHourEndUtc(now = new Date()) {
  // "fim" da última hora COMPLETA no horário BR, convertido para UTC
  const nowBr = new Date(now.getTime() - BR_OFFSET_MS);
  nowBr.setMinutes(0, 0, 0); // início da hora atual BR
  return new Date(nowBr.getTime() + BR_OFFSET_MS); // UTC
}

function hourStartUtcFromEndUtc(endUtc) {
  return new Date(endUtc.getTime() - 3600_000);
}

// maior ts (UTC) já fechado no rollup (accel/freq)
async function getLastClosedHourEndUtc(db) {
  const colA = db.collection("telemetry_rollup_hourly_accel");
  const colF = db.collection("telemetry_rollup_hourly_freq");

  const lastA = await colA.find({}, { projection: { ts: 1 } }).sort({ ts: -1 }).limit(1).next();
  const lastF = await colF.find({}, { projection: { ts: 1 } }).sort({ ts: -1 }).limit(1).next();

  const tsA = lastA?.ts ? new Date(lastA.ts) : null;
  const tsF = lastF?.ts ? new Date(lastF.ts) : null;

  if (!tsA && !tsF) return null;
  if (tsA && tsF) return new Date(Math.max(tsA.getTime(), tsF.getTime()));
  return tsA || tsF;
}

// ✅ fecha todas as horas completas faltantes desde o último rollup até agora
async function catchUpHourly(db) {
  const lastEndUtc = await getLastClosedHourEndUtc(db);
  const targetEndUtc = lastCompleteHourEndUtc(new Date());

  if (!lastEndUtc) {
    console.log("[hotHourly] catch-up: rollup vazio (sem ts). Use backfill se necessário.");
    return;
  }

  let endUtc = new Date(lastEndUtc.getTime() + 3600_000);
  let n = 0;

  while (endUtc <= targetEndUtc) {
    const startUtc = hourStartUtcFromEndUtc(endUtc);

    console.log("[hotHourly] catch-up closing", startUtc.toISOString(), "->", endUtc.toISOString());

    // ✅ assinatura correta (db, {fromUtc,toUtc})
    await rollupHourlyAccel(db, { fromUtc: startUtc, toUtc: endUtc });
    await rollupHourlyFreq(db, { fromUtc: startUtc, toUtc: endUtc });

    n++;
    endUtc = new Date(endUtc.getTime() + 3600_000);
  }

  console.log(`[hotHourly] catch-up done. Hours closed: ${n}`);
}

// log de “quanto falta”
function secondsUntilNextRunBR() {
  const now = new Date();
  const nextEndUtc = lastCompleteHourEndUtc(now);     // início da hora atual BR em UTC
  const nextRunUtc = new Date(nextEndUtc.getTime() + 10_000); // HH:00:10 BR (em UTC)
  let diff = Math.floor((nextRunUtc.getTime() - now.getTime()) / 1000);
  if (diff < 0) diff += 3600; // se passou, pega a próxima
  return diff;
}

export async function startHotHourly(dbFromBoot) {
  const enabled = (process.env.START_ROLLUPS || "true").toLowerCase() === "true";
  if (!enabled) {
    console.log("[hotHourly] disabled (START_ROLLUPS=false)");
    return;
  }

  // usa o db que veio do boot (preferível)
  const db = dbFromBoot || mongoose.connection.db;

  // ✅ 1) recupera horas perdidas ao subir
  await catchUpHourly(db);

  console.log("[hotHourly] next run in", secondsUntilNextRunBR(), "seconds");

  // ✅ 2) cron HH:00:10 no fuso BR
  // node-cron com 6 campos: second minute hour day month weekday
  cron.schedule(
    "10 0 * * * *",
    async () => {
      try {
        const endUtc = lastCompleteHourEndUtc(new Date());
        const startUtc = hourStartUtcFromEndUtc(endUtc);

        console.log("[hotHourly] closing hour", startUtc.toISOString(), "->", endUtc.toISOString());

        const t0 = Date.now();
        await rollupHourlyAccel(db, { fromUtc: startUtc, toUtc: endUtc });
        const accelMs = Date.now() - t0;

        const t1 = Date.now();
        await rollupHourlyFreq(db, { fromUtc: startUtc, toUtc: endUtc });
        const freqMs = Date.now() - t1;

        console.log(`[hotHourly] closed hour accel=${accelMs}ms freq=${freqMs}ms`);
        console.log("[hotHourly] next run in", secondsUntilNextRunBR(), "seconds");
      } catch (err) {
        console.error("[hotHourly] error", err?.message || err);
      }
    },
    { timezone: TZ_BR }
  );

  console.log("[hotHourly] started (cron BR HH:00:10 + catch-up on boot)");
}