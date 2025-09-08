
import { db } from "../lib/db.js";
const ACCEL = "telemetry_ts_accel";
const FREQ  = "telemetry_ts_freq_peaks";
const STAT  = "telemetry_ts_device_status";

export async function ensureTimeSeries() {
  const conn = db();
  const existing = new Set((await conn.db.listCollections().toArray()).map(c => c.name));

  async function ensure(name, spec) {
    if (!existing.has(name)) {
      await conn.db.createCollection(name, spec);
      console.log("Created", name);
    } else {
      console.log("OK", name);
    }
  }

  const tsSpec = (timeField, metaField) => ({
    timeseries: { timeField, metaField, granularity: "seconds" }
  });

  await ensure(ACCEL, tsSpec("ts","meta"));
  await ensure(FREQ,  tsSpec("ts","meta"));
  await ensure(STAT,  tsSpec("ts","meta"));
}

if (import.meta.url === `file://${process.argv[1]}`) {
  import("dotenv").then(async ({ default: dotenv }) => {
    dotenv.config();
    const { connectMongo } = await import("../lib/db.js");
    await connectMongo(process.env.MONGO_URI);
    await ensureTimeSeries();
    process.exit(0);
  });
}
