// src/bootstrap/mongoSetup.js
// Bootstrap do Mongo: cria coleções, índices únicos e TTLs.
// Usa horas BR no rollup depois, mas aqui é só infra.

const DAY = 24 * 60 * 60;

// coleções RAW (timeseries) que você já tem
const RAW_ACCEL = "telemetry_ts_accel";
const RAW_FREQ = "telemetry_ts_freq_peaks";

// coleções HOT rollup (vamos criar)
const HOURLY_ACCEL = "telemetry_rollup_hourly_accel";
const HOURLY_FREQ  = "telemetry_rollup_hourly_freq";
const DAILY_ACCEL  = "telemetry_rollup_daily_accel";
const DAILY_FREQ   = "telemetry_rollup_daily_freq";

// TTLs (em segundos)
const TTL_RAW_SECONDS    = 5 * DAY;   // 5 dias
const TTL_HOURLY_SECONDS = 7 * DAY;   // 7 dias
const TTL_DAILY_SECONDS  = 10 * DAY;  // 10 dias

async function collectionExists(db, name) {
  const cols = await db.listCollections({ name }).toArray();
  return cols.length > 0;
}

async function ensureCollection(db, name) {
  const exists = await collectionExists(db, name);
  if (!exists) {
    await db.createCollection(name);
    console.log(`[mongoSetup] createCollection: ${name}`);
  }
  return db.collection(name);
}

async function ensureIndex(col, keys, options) {
  try {
    await col.createIndex(keys, options);
    console.log(`[mongoSetup] createIndex: ${col.collectionName}`, keys, options?.name || "");
  } catch (e) {
    console.warn(`[mongoSetup] WARN createIndex ${col.collectionName}:`, e?.message || e);
  }
}

// Tenta configurar expiração nas collections TIMESERIES via collMod.
// Se falhar (permissão/versão/etc), a gente só avisa e segue.
async function trySetTimeseriesExpire(db, collName, expireAfterSeconds) {
  try {
    // collMod é o jeito “certo” de expirar timeseries
    await db.command({
      collMod: collName,
      expireAfterSeconds,
    });
    console.log(`[mongoSetup] collMod expireAfterSeconds OK: ${collName} = ${expireAfterSeconds}s`);
  } catch (e) {
    console.warn(
      `[mongoSetup] WARN: não consegui aplicar collMod expireAfterSeconds em ${collName}. ` +
      `Você pode ignorar por enquanto e a gente resolve depois. Detalhe: ${e?.message || e}`
    );
  }
}

export async function mongoSetup(db) {
  console.log("[mongoSetup] início");

  // 1) RAW timeseries: tentar manter apenas 5 dias
  await trySetTimeseriesExpire(db, RAW_ACCEL, TTL_RAW_SECONDS);
  await trySetTimeseriesExpire(db, RAW_FREQ, TTL_RAW_SECONDS);

  // 2) Criar coleções de rollup (se não existirem)
  const cHourlyAccel = await ensureCollection(db, HOURLY_ACCEL);
  const cHourlyFreq  = await ensureCollection(db, HOURLY_FREQ);
  const cDailyAccel  = await ensureCollection(db, DAILY_ACCEL);
  const cDailyFreq   = await ensureCollection(db, DAILY_FREQ);

  // 3) Índices únicos (idempotência)
  // Accel hourly: company_id + bridge_id + device_id + axis + bucket_br
  await ensureIndex(cHourlyAccel,
    { company_id: 1, bridge_id: 1, device_id: 1, axis: 1, bucket_br: 1 },
    { unique: true, name: "uniq_hourly_accel_bucket" }
  );

  // Freq hourly: company_id + bridge_id + device_id + stream + bucket_br
  await ensureIndex(cHourlyFreq,
    { company_id: 1, bridge_id: 1, device_id: 1, stream: 1, bucket_br: 1 },
    { unique: true, name: "uniq_hourly_freq_bucket" }
  );

  // Accel daily: company_id + bridge_id + device_id + axis + bucket_br
  await ensureIndex(cDailyAccel,
    { company_id: 1, bridge_id: 1, device_id: 1, axis: 1, bucket_br: 1 },
    { unique: true, name: "uniq_daily_accel_bucket" }
  );

  // Freq daily: company_id + bridge_id + device_id + stream + bucket_br
  await ensureIndex(cDailyFreq,
    { company_id: 1, bridge_id: 1, device_id: 1, stream: 1, bucket_br: 1 },
    { unique: true, name: "uniq_daily_freq_bucket" }
  );

  // 4) TTL dos rollups
  // TTL index precisa ser em um campo Date. Vamos usar bucket_br (Date).
  // expireAfterSeconds conta a partir de bucket_br.
  await ensureIndex(cHourlyAccel,
    { bucket_br: 1 },
    { expireAfterSeconds: TTL_HOURLY_SECONDS, name: "ttl_hourly_accel_bucket" }
  );
  await ensureIndex(cHourlyFreq,
    { bucket_br: 1 },
    { expireAfterSeconds: TTL_HOURLY_SECONDS, name: "ttl_hourly_freq_bucket" }
  );
  await ensureIndex(cDailyAccel,
    { bucket_br: 1 },
    { expireAfterSeconds: TTL_DAILY_SECONDS, name: "ttl_daily_accel_bucket" }
  );
  await ensureIndex(cDailyFreq,
    { bucket_br: 1 },
    { expireAfterSeconds: TTL_DAILY_SECONDS, name: "ttl_daily_freq_bucket" }
  );

  console.log("[mongoSetup] fim");
}