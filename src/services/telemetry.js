
import mongoose from "mongoose";

const ACCEL = "telemetry_ts_accel";
const FREQ  = "telemetry_ts_freq_peaks";
const STAT  = "telemetry_ts_device_status";

export async function insertAccel({ company_id, bridge_id, device_id, ts, axis = "z", value, fw }) {
  const doc = {
    ts: ts ? new Date(ts) : new Date(),
    meta: { company_id, bridge_id, device_id, stream: `accel:${axis}` },
    value, units: "m/s2", fw
  };
  return mongoose.connection.db.collection(ACCEL).insertOne(doc);
}

export async function insertFreqPeaks({ company_id, bridge_id, device_id, ts, status, fs, n, peaks, fw }) {
  const doc = {
    ts: ts ? new Date(ts) : new Date(),
    meta: { company_id, bridge_id, device_id, stream: "freq:z" },
    status, fs, n, peaks, fw
  };
  return mongoose.connection.db.collection(FREQ).insertOne(doc);
}

export async function insertDeviceStatus({ company_id, bridge_id, device_id, ts, status, rssi, battery_v }) {
  const doc = {
    ts: ts ? new Date(ts) : new Date(),
    meta: { company_id, bridge_id, device_id },
    status, rssi, battery_v
  };
  return mongoose.connection.db.collection(STAT).insertOne(doc);
}
