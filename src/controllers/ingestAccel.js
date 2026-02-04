// src/controllers/ingestAccel.js
import Joi from "joi";
import mongoose from "mongoose";
import Device from "../models/device.js";
import BridgeLimit from "../models/bridgeLimit.js";
import Alert from "../models/alert.js";
import Recipient from "../models/recipient.js";
import TsAccel from "../models/tsAccel.js";
import { toBrazilISOFromUTC } from "../lib/time.js";
import { sendWebPushToRecipient, sendSMS } from "../services/notify.js";

const accelSchema = Joi.object({
  device_id: Joi.string().required(),
  ts: Joi.alternatives(Joi.date(), Joi.string(), Joi.number()).optional(),
  axis: Joi.string().valid("x", "y", "z").default("z"),
  value: Joi.number().required()
});

// ---- Limites por dispositivo com fallback para BridgeLimit
function resolveAccelLimitsFromDevice(dev, bridgeLim) {
  const dl =
    dev?.limits?.accel ||
    dev?.params_current?.limits?.accel ||
    {};

  return {
    max_warning: dl.max_warning ?? dl.alert ?? bridgeLim?.accel_alert ?? 10.0,
    max_critical: dl.max_critical ?? dl.critical ?? bridgeLim?.accel_critical ?? 12.0,
    min_warning: dl.min_warning ?? dl.min_alert ?? bridgeLim?.accel_min_alert ?? null,
    min_critical: dl.min_critical ?? dl.min_critical ?? bridgeLim?.accel_min_critical ?? null
  };
}

function classifyTwoSidedWarning(x, lim) {
  const v = Math.abs(x);
  if ((lim.max_critical != null && v >= lim.max_critical) ||
    (lim.min_critical != null && v >= lim.min_critical)) return "critical";
  if ((lim.max_warning != null && v >= lim.max_warning) ||
    (lim.min_warning != null && v >= lim.min_warning)) return "warning";
  return "normal";
}

async function notifyBridge(bridge_id, payload, severity) {
  try {
    const recips = await Recipient.find({ bridge_id, active: true, severity });
    for (const r of recips) {
      if (r.channels?.includes("push")) await sendWebPushToRecipient(r._id, payload);
      if (r.channels?.includes("sms") && r.phone) await sendSMS(r.phone, `${payload.title}: ${payload.body}`);
    }
  } catch (e) {
    console.error("notifyBridge error:", e?.message || e);
  }
}

export async function ingestAccel(req, res, next) {
  try {
    const { value: body, error } = accelSchema.validate(req.body, { stripUnknown: true });
    if (error) return res.status(400).json({ error: error.message });

    const dev = await Device.findOne({ device_id: body.device_id });
    if (!dev) return res.status(404).json({ error: "Unknown device_id" });

    const tsUTC = body.ts ? new Date(body.ts) : new Date();
    const ts_br = toBrazilISOFromUTC(tsUTC);

    const bridgeLim = await BridgeLimit.findOne({ bridge_id: dev.bridge_id });
    const lim = resolveAccelLimitsFromDevice(dev, bridgeLim);
    const severity = classifyTwoSidedWarning(body.value, lim);

    const docId = new mongoose.Types.ObjectId();
    await TsAccel.create({
      _id: docId,
      device_id: body.device_id,
      meta: {
        object_id: docId,
        company_id: dev.company_id,
        bridge_id: dev.bridge_id,
        device_id: dev.device_id,
        axis: body.axis || "z",
      },
      ts: tsUTC,
      ts_br,
      value: body.value,
      severity
    });

    await Device.updateOne(
      { device_id: body.device_id },
      { $set: { last_seen: tsUTC } }
    );

    // ✅ NOVO: envio em tempo real via WebSocket (ACCEL)
    const io = globalThis.__io;
    if (io) {
      io.to(`bridge:${String(dev.bridge_id)}`).emit("telemetry", {
        type: "accel",
        bridge_id: String(dev.bridge_id),
        company_id: String(dev.company_id),
        device_id: body.device_id,
        ts: tsUTC.toISOString(),
        payload: {
          axis: body.axis || "z",
          value: body.value ?? null,
          severity
        }
      });
    }


    if (severity !== "normal") {
      const rotulo = severity === "critical" ? "CRÍTICO" : "AVISO";
      const ref = severity === "critical"
        ? (lim.max_critical ?? lim.min_critical)
        : (lim.max_warning ?? lim.min_warning);

      const message = `Aceleração ${body.value.toFixed(2)} m/s² > limite ${rotulo} (${ref} m/s²)`;

      // await Alert.create({
      //   company_id: dev.company_id,
      //   bridge_id: dev.bridge_id,
      //   device_id: dev.device_id,
      //   type: "accel",
      //   severity,
      //   message,
      //   payload: { value: body.value, axis: body.axis || "z", ts: tsUTC, limits: lim }
      // });

      // await notifyBridge(dev.bridge_id, { title: `Aceleração (${rotulo})`, body: message, severity }, severity);
    }

    return res.status(201).json({ ok: true, id: docId.toString() });
  } catch (e) {
    next(e);
  }
}

