// src/controllers/ingestAccel.js
import Joi from "joi";
import mongoose from "mongoose";
import Device from "../models/device.js";
import BridgeLimit from "../models/bridgeLimit.js";
import Alert from "../models/alert.js";
import Recipient from "../models/recipient.js";
import { classifyTwoSided } from "../lib/limits.js";
import { toBrazilISOFromUTC } from "../lib/time.js";
import { sendWebPushToRecipient, sendSMS } from "../services/notify.js";

// Schema do payload recebido do dispositivo
const accelSchema = Joi.object({
  device_id: Joi.string().required(),
  ts: Joi.alternatives(Joi.date(), Joi.string(), Joi.number()).optional(), // timestamp opcional (UTC)
  axis: Joi.string().valid("x", "y", "z").default("z"),
  value: Joi.number().required(), // m/s^2
  fw: Joi.string().optional()
});

// Envia notificações aos destinatários da ponte (se configurados)
async function notifyBridge(bridge_id, payload, severity) {
  try {
    const recips = await Recipient.find({ bridge_id, active: true, severity });
    for (const r of recips) {
      if (r.channels?.includes("push")) {
        await sendWebPushToRecipient(r._id, payload);
      }
      if (r.channels?.includes("sms") && r.phone) {
        await sendSMS(r.phone, `${payload.title}: ${payload.body}`);
      }
    }
  } catch (e) {
    // Notificações não devem quebrar o ingest
    console.error("notifyBridge error:", e?.message || e);
  }
}

export async function ingestAccel(req, res, next) {
  try {
    // 1) Validação
    const { value: body, error } = accelSchema.validate(req.body, {
      stripUnknown: true
    });
    if (error) return res.status(400).json({ error: error.message });

    // 2) Dispositivo
    const dev = await Device.findOne({ device_id: body.device_id });
    if (!dev) return res.status(404).json({ error: "Unknown device_id" });

    // 3) Tempo (UTC + Brasil)
    const tsUTC = body.ts ? new Date(body.ts) : new Date(); // Date em UTC
    const ts_br = toBrazilISOFromUTC(tsUTC);                // string "YYYY-MM-DDTHH:mm:ss-03:00"
    const date_br = ts_br.slice(0, 10);
    const hour_br = Number(ts_br.slice(11, 13));

    // 4) Limites (alerta/critico; min opcional p/ 2-sided)
    const lim = await BridgeLimit.findOne({ bridge_id: dev.bridge_id });
    const accelAlert = lim?.accel_alert ?? 10.0;
    const accelCrit  = lim?.accel_critical ?? 12.0;
    const minAlert   = lim?.accel_min_alert ?? null;
    const minCrit    = lim?.accel_min_critical ?? null;

    // Classificação (usa valor absoluto por padrão)
    const sev = classifyTwoSided(
      Math.abs(body.value),
      minAlert, minCrit,
      accelAlert, accelCrit
    ); // "normal" | "alert" | "critical"

    // 5) Persistência APENAS no time-series
    await mongoose.connection.db
      .collection("telemetry_ts_accel")
      .insertOne({
        meta: {
          company_id: dev.company_id,
          bridge_id:  dev.bridge_id,
          device_id:  dev.device_id,
          axis: body.axis || "z"
        },
        ts: tsUTC,         // <- timestamp UTC (Date)
        ts_br,             // <- timestamp no fuso de Brasília (string)
        date_br,           // útil para filtros diários
        hour_br,           // útil para agregações por hora
        value: body.value, // m/s^2
        fw: body.fw || dev.firmware_version
      });

    // 6) Alertas (opcional, mas preservado)
    if (sev !== "normal") {
      const rotulo = sev === "critical" ? "CRÍTICO" : "ALERTA";
      const limiteUsado =
        sev === "critical" ? (accelCrit ?? accelAlert) : (accelAlert ?? accelCrit);
      const message =
        `Aceleração ${body.value.toFixed(2)} m/s² > limite ${rotulo} (${limiteUsado} m/s²)`;

      await Alert.create({
        company_id: dev.company_id,
        bridge_id:  dev.bridge_id,
        device_id:  dev.device_id,
        type: "accel",
        severity: sev,
        message,
        payload: {
          value: body.value,
          axis: body.axis || "z",
          ts: tsUTC,
          limits: {
            accel_alert: accelAlert,
            accel_critical: accelCrit,
            accel_min_alert: minAlert,
            accel_min_critical: minCrit,
            version: lim?.config_version ?? 1
          }
        }
      });

      await notifyBridge(
        dev.bridge_id,
        { title: `Aceleração (${rotulo})`, body: message, severity: sev },
        sev
      );
    }

    return res.status(201).json({ ok: true });
  } catch (e) {
    next(e);
  }
}
