import Joi from "joi";
import Device from "../models/device.js";
import BridgeLimit from "../models/bridgeLimit.js";
import Alert from "../models/alert.js";
import AccelData from "../models/accelData.js";
import { classifyTwoSided } from "../lib/limits.js";
import { toBrazilISOFromUTC } from "../lib/time.js";
// se usar notifyBridge aqui, importe ou defina a função
import { sendWebPushToRecipient, sendSMS } from "../services/notify.js";
import Recipient from "../models/recipient.js";

const accelSchema = Joi.object({
  device_id: Joi.string().required(),
  ts: Joi.alternatives(Joi.date(), Joi.string(), Joi.number()).optional(),
  axis: Joi.string().valid("x","y","z").default("z"),
  value: Joi.number().required(),
  fw: Joi.string().optional()
});

async function notifyBridge(bridge_id, payload, severity) {
  const recipients = await Recipient.find({ bridge_id, active: true, severity });
  for (const r of recipients) {
    if (r.channels?.includes("push"))
      await sendWebPushToRecipient(r._id, payload);
    if (r.channels?.includes("sms") && r.phone)
      await sendSMS(r.phone, `${payload.title}: ${payload.body}`);
  }
}

export async function ingestAccel(req, res, next) {
  try {
    const { value, error } = accelSchema.validate(req.body, { stripUnknown: true });
    if (error) return res.status(400).json({ error: error.message });
    const dev = await Device.findOne({ device_id: value.device_id });
    if (!dev) return res.status(404).json({ error: "Unknown device_id" });

    const tsUTC = value.ts ? new Date(value.ts) : new Date();
    const ts_br = toBrazilISOFromUTC(tsUTC);
    const date_br = ts_br.slice(0,10);
    const hour_br = Number(ts_br.slice(11,13));

    const lim = await BridgeLimit.findOne({ bridge_id: dev.bridge_id });
    const accelAlert = lim?.accel_alert ?? 10.0;
    const accelCrit  = lim?.accel_critical ?? 12.0;
    const minAlert   = lim?.accel_min_alert ?? null;
    const minCrit    = lim?.accel_min_critical ?? null;

    const sev = classifyTwoSided(
      Math.abs(value.value),         // aceleração: geralmente usamos |valor|
      minAlert, minCrit,
      accelAlert, accelCrit
    );

    const doc = await AccelData.create({
      company_id: dev.company_id,
      bridge_id:  dev.bridge_id,
      device_id:  value.device_id,
      ts: tsUTC, ts_br, date_br, hour_br,
      axis: value.axis,
      value: value.value,
      fw: value.fw || dev.firmware_version,
      severity: sev,
      limits: {
        accel_alert: accelAlert,
        accel_critical: accelCrit,
        accel_min_alert: minAlert,
        accel_min_critical: minCrit,
        version: lim?.config_version ?? 1
      }
    });

    if (sev !== "normal") {
      const rotulo = sev === "critical" ? "CRÍTICO" : "ALERTA";
      const limiteUsado = sev === "critical"
        ? (accelCrit ?? accelAlert)
        : (accelAlert ?? accelCrit);
      const message = `Aceleração ${value.value.toFixed(2)} m/s² > limite ${rotulo} (${limiteUsado} m/s²)`;

      await Alert.create({
        company_id: dev.company_id, bridge_id: dev.bridge_id, device_id: dev.device_id,
        type: "accel", severity: sev, message,
        payload: { value: value.value, limits: doc.limits }
      });

      await notifyBridge(dev.bridge_id, { title: `Aceleração (${rotulo})`, body: message, severity: sev }, sev);
    }

    res.status(201).json({ ok: true });
  } catch (e) { next(e); }
}
