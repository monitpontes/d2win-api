// src/crons/exportD5.js
import cron from "node-cron";
import { exportD5ToS3 } from "../services/export/exportDayD5.js";
import { existsObject } from "../services/s3Objects.js";

const TZ_BR = "America/Sao_Paulo";
const CONTROL_COLL = "export_jobs";

// quantos dias máximos o catch-up tenta recuperar ao subir
const MAX_CATCHUP_DAYS = Math.max(1, Number(process.env.EXPORT_CATCHUP_MAX_DAYS || 10));

function brNowDate() {
  // “Date” representando o relógio BR (sem depender do TZ do servidor)
  return new Date(new Date().toLocaleString("en-US", { timeZone: TZ_BR }));
}

function formatYYYYMMDD(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/**
 * Qual é o dia BR que deve ser exportado hoje?
 * Ex: hoje BR=2026-03-06 => exporta 2026-03-01 (D-5)
 */
function computeTargetDayBrString(offsetDays = 5, baseBr = brNowDate()) {
  const d = new Date(baseBr);
  d.setHours(0, 0, 0, 0);
  d.setDate(d.getDate() - offsetDays);
  return formatYYYYMMDD(d);
}

/**
 * Evita duplicar: grava que exportamos dayBr.
 */
async function alreadyDone(db, dayBr) {
  const col = db.collection(CONTROL_COLL);
  const doc = await col.findOne({ job: "export_d5", day_br: dayBr, done: true });
  return !!doc;
}

async function markDone(db, dayBr, payload) {
  const col = db.collection(CONTROL_COLL);
  await col.updateOne(
    { job: "export_d5", day_br: dayBr },
    {
      $set: {
        job: "export_d5",
        day_br: dayBr,
        done: true,
        updated_at: new Date(),
        ...(payload || {}),
      },
      $setOnInsert: { created_at: new Date() },
    },
    { upsert: true }
  );
}

/**
 * Descobre o último day_br exportado (controle), pra fazer catch-up.
 */
async function getLastDoneDayBr(db) {
  const col = db.collection(CONTROL_COLL);
  const doc = await col
    .find({ job: "export_d5", done: true }, { projection: { day_br: 1 } })
    .sort({ day_br: -1 })
    .limit(1)
    .next();
  return doc?.day_br || null; // string YYYY-MM-DD
}

function addDaysToDayStr(dayStr, n) {
  // dayStr "YYYY-MM-DD" em BR
  const [Y, M, D] = dayStr.split("-").map(Number);
  const d = new Date(Y, M - 1, D);
  d.setDate(d.getDate() + n);
  return formatYYYYMMDD(d);
}

/** monta a key do S3 exatamente no padrão do export */
function s3Key(domain, type, dayBr) {
  const [Y, M] = dayBr.split("-"); // YYYY-MM-DD
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

/**
 * ✅ idempotência forte: se já existe no S3, não exporta de novo.
 * Retorna:
 *  - allExist: boolean
 *  - existsByKey: { [key]: true/false }
 */
async function alreadyExportedInS3(dayBr) {
  const keys = keysForDay(dayBr);
  const results = await Promise.all(keys.map(async (k) => [k, await existsObject(k)]));
  const existsByKey = Object.fromEntries(results);
  const allExist = keys.every((k) => !!existsByKey[k]);
  return { allExist, existsByKey };
}

/**
 * Executa 1 export “do diaBr”.
 * Como seu exportD5ToS3 usa offsetDays, aqui a gente calcula o offset dinâmico
 * em relação ao “hoje BR”.
 */
async function exportSpecificDayBr(db, dayBr) {
  // calcula offsetDays relativo a hoje BR
  const todayBr = brNowDate();
  todayBr.setHours(0, 0, 0, 0);

  const [Y, M, D] = dayBr.split("-").map(Number);
  const target = new Date(Y, M - 1, D);
  target.setHours(0, 0, 0, 0);

  const diffDays = Math.round((todayBr.getTime() - target.getTime()) / 86400000);

  if (diffDays < 5) {
    return { exported: false, reason: "dayBr é recente (diffDays<5), não exporta", dayBr };
  }

  // 1) se já marcado no controle, skip
  if (await alreadyDone(db, dayBr)) {
    return { exported: false, reason: "já exportado (controle)", dayBr };
  }

  // 2) ✅ se já existe no S3 (mesmo que exportou manualmente), skip e marca done
  const s3Check = await alreadyExportedInS3(dayBr);
  if (s3Check.allExist) {
    await markDone(db, dayBr, {
      last_result: "skip_s3_already_exists",
      s3_exists: s3Check.existsByKey,
    });
    return { exported: false, reason: "já existe no S3 (todos os arquivos)", dayBr };
  }

  // 3) chama teu export padrão por offset
  const res = await exportD5ToS3(db, { offsetDays: diffDays });

  if (res?.exported) {
    // após export, re-checa e grava o mapa do S3
    const s3After = await alreadyExportedInS3(dayBr);

    await markDone(db, dayBr, {
      last_result: "exported",
      meta: res,
      s3_exists: s3After.existsByKey,
    });
  }

  return res;
}

async function catchUpExportD5(db) {
  const targetMostRecent = computeTargetDayBrString(5);
  const lastDone = await getLastDoneDayBr(db);

  if (!lastDone) {
    console.log("[exportD5] catch-up: nenhum export ainda (controle vazio). Nada a recuperar automaticamente.");
    return;
  }

  let cursor = addDaysToDayStr(lastDone, 1);
  let n = 0;

  while (cursor <= targetMostRecent && n < MAX_CATCHUP_DAYS) {
    console.log("[exportD5] catch-up trying dayBr=", cursor);
    const r = await exportSpecificDayBr(db, cursor);

    if (!r?.exported) {
      console.log("[exportD5] catch-up skip:", r?.reason, "dayBr=", cursor);
    } else {
      console.log("[exportD5] catch-up exported dayBr=", cursor);
    }

    cursor = addDaysToDayStr(cursor, 1);
    n++;
  }

  console.log("[exportD5] catch-up done. Attempts:", n, "max:", MAX_CATCHUP_DAYS);
}

function secondsUntilNextRunBR() {
  const nowBr = brNowDate();
  const next = new Date(nowBr);
  next.setHours(0, 0, 10, 0); // 00:00:10 BR
  if (next <= nowBr) next.setDate(next.getDate() + 1);
  return Math.max(0, Math.floor((next.getTime() - nowBr.getTime()) / 1000));
}

export async function startExportD5(nativeDb) {
  const enabled = (process.env.START_EXPORTS || "true").toLowerCase() === "true";
  if (!enabled) {
    console.log("[exportD5] disabled (START_EXPORTS=false)");
    return;
  }

  // ✅ garante índice único do controle
  await nativeDb.collection(CONTROL_COLL).createIndex(
    { job: 1, day_br: 1 },
    { unique: true, name: "uniq_export_job_day" }
  );

  // 1) catch-up ao subir
  await catchUpExportD5(nativeDb);

  console.log("[exportD5] next run in", secondsUntilNextRunBR(), "seconds");

  // 2) rotina diária
  cron.schedule(
    "10 0 0 * * *", // 00:00:10 BR
    async () => {
      try {
        const dayBr = computeTargetDayBrString(5);
        console.log("[exportD5] scheduled target dayBr=", dayBr);

        const r = await exportSpecificDayBr(nativeDb, dayBr);

        if (!r?.exported) {
          console.log("[exportD5] skipped:", r?.reason, "dayBr=", dayBr);
        } else {
          console.log("[exportD5] exported dayBr=", dayBr);
        }

        console.log("[exportD5] next run in", secondsUntilNextRunBR(), "seconds");
      } catch (e) {
        console.error("[exportD5] error:", e?.message || e);
      }
    },
    { timezone: TZ_BR }
  );

  console.log("[exportD5] started (cron BR 00:00:10 + catch-up on boot)");
}