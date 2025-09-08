
import Device from "../models/device.js";
import { insertAccel, insertFreqPeaks } from "../services/telemetry.js";

function mapCollectionToDeviceId(collection) {
  if (!collection) return null;
  const m = collection.match(/_(\w+)$/);
  if (m) return `S${m[1]}`;
  return null;
}

export async function postAcceleration(req, res, next) {
  try {
    let { device_id, collection, accelZ, ts, axis, fw } = req.body;
    device_id = device_id || mapCollectionToDeviceId(collection);
    if (!device_id) return res.status(400).json({ error: "device_id or collection required" });
    if (typeof accelZ !== "number") return res.status(400).json({ error: "accelZ must be number" });

    const dev = await Device.findOne({ device_id });
    if (!dev) return res.status(404).json({ error: "Unknown device_id" });

    await insertAccel({
      company_id: dev.company_id, bridge_id: dev.bridge_id, device_id, ts,
      axis: axis || "z", value: accelZ, fw: fw || dev.firmware_version
    });
    res.status(201).json({ ok: true });
  } catch (e) { next(e); }
}

export async function postFrequency(req, res, next) {
  try {
    let { device_id, collection, status, freqPico1, magPico1, freqPico2, magPico2, ts, fs, n, fw } = req.body;
    device_id = device_id || mapCollectionToDeviceId(collection);
    if (!device_id) return res.status(400).json({ error: "device_id or collection required" });

    const dev = await Device.findOne({ device_id });
    if (!dev) return res.status(404).json({ error: "Unknown device_id" });

    const peaks = [];
    if (typeof freqPico1 === "number" && typeof magPico1 === "number") peaks.push({ f: freqPico1, mag: magPico1 });
    if (typeof freqPico2 === "number" && typeof magPico2 === "number") peaks.push({ f: freqPico2, mag: magPico2 });

    await insertFreqPeaks({
      company_id: dev.company_id, bridge_id: dev.bridge_id, device_id, ts,
      status: status || "atividade_detectada", fs: fs || 50, n: n || 4096, peaks, fw: fw || dev.firmware_version
    });
    res.status(201).json({ ok: true });
  } catch (e) { next(e); }
}
