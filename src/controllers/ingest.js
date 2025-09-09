
// import Joi from "joi";
// import Device from "../models/device.js";
// import BridgeLimit from "../models/bridgeLimit.js";
// import Alert from "../models/alert.js";
// import Recipient from "../models/recipient.js";
// import { insertAccel, insertFreqPeaks } from "../services/telemetry.js";
// import { sendWebPushToRecipient, sendSMS } from "../services/notify.js";

// const accelSchema = Joi.object({ 
//   device_id: Joi.string().required(), 
//   ts: Joi.date().optional(),
//   axis: Joi.string().valid("x","y","z").default("z"),
//   value: Joi.number().required(),
//   fw: Joi.string().optional()
// });

// const freqSchema = Joi.object({
//   device_id: Joi.string().required(),
//   ts: Joi.date().optional(), 
//   status: Joi.string().valid("atividade_detectada","sem_atividade").required(),
//   fs: Joi.number().required(),
//   n: Joi.number().required(),
//   peaks: Joi.array().items(Joi.object({ f: Joi.number().required(), mag: Joi.number().required() })).default([]),
//   fw: Joi.string().optional()
// });

// async function notifyBridge(bridge_id, payload, severity) {
//   const recipients = await Recipient.find({ bridge_id, active: true, severity });
//   for (const r of recipients) {
//     if (r.channels?.includes("push")) {
//       await sendWebPushToRecipient(r._id, payload);
//     }
//     if (r.channels?.includes("sms") && r.phone) {
//       await sendSMS(r.phone, `${payload.title}: ${payload.body}`);
//     }
//   }
// }

// export async function ingestAccel(req, res, next) {
//   try {
//     const { value, error } = accelSchema.validate(req.body, { stripUnknown: true });
//     if (error) return res.status(400).json({ error: error.message });

//     const dev = await Device.findOne({ device_id: value.device_id });
//     if (!dev) return res.status(404).json({ error: "Unknown device_id" });

//     await insertAccel({
//       company_id: dev.company_id, bridge_id:  dev.bridge_id, device_id:  value.device_id,
//       ts: value.ts, axis: value.axis, value: value.value, fw: value.fw || dev.firmware_version
//     });

//     // threshold check (accel)
//     const lim = await BridgeLimit.findOne({ bridge_id: dev.bridge_id });
//     const accelMax = lim?.accel_max_ms2 ?? 2.0; 
//     if (Math.abs(value.value) > accelMax) {
//       const message = `Aceleração ${value.value.toFixed(2)} m/s² > limite ${accelMax} m/s²`;
//       await Alert.create({
//         company_id: dev.company_id, bridge_id: dev.bridge_id, device_id: dev.device_id,
//         type: "accel", severity: "warning", message, payload: { value: value.value }
//       });
//       await notifyBridge(dev.bridge_id, { title: "Alerta de aceleração", body: message, severity: "warning" }, "warning");
//     }

//     res.status(201).json({ ok: true });
//   } catch (e) { next(e); }
// }

// export async function ingestFrequency(req, res, next) {
//   try {
//     const { value, error } = freqSchema.validate(req.body, { stripUnknown: true });
//     if (error) return res.status(400).json({ error: error.message });

//     const dev = await Device.findOne({ device_id: value.device_id });
//     if (!dev) return res.status(404).json({ error: "Unknown device_id" });

//     await insertFreqPeaks({
//       company_id: dev.company_id, bridge_id:  dev.bridge_id, device_id:  value.device_id,
//       ts: value.ts, status: value.status, fs: value.fs, n: value.n, peaks: value.peaks,
//       fw: value.fw || dev.firmware_version
//     });

//     // threshold check (freq) - default 4 Hz if not configured
//     const lim = await BridgeLimit.findOne({ bridge_id: dev.bridge_id });
//     const freqMax = lim?.freq_max_hz ?? 4.0;
//     const maxPeak = Math.max(0, ...value.peaks.map(p => p.f));
//     if (maxPeak > freqMax) {
//       const message = `Frequência pico ${maxPeak.toFixed(2)} Hz > limite ${freqMax} Hz`;
//       await Alert.create({
//         company_id: dev.company_id, bridge_id: dev.bridge_id, device_id: dev.device_id,
//         type: "freq", severity: "critical", message, payload: { peaks: value.peaks }
//       });
//       await notifyBridge(dev.bridge_id, { title: "Alerta de frequência", body: message, severity: "critical" }, "critical");
//     }

//     res.status(201).json({ ok: true });
//   } catch (e) { next(e); }
// }
