// src/controllers/ingestFreq.js
import Joi from "joi";
import Device from "../models/device.js";
import BridgeLimit from "../models/bridgeLimit.js";
import Alert from "../models/alert.js";
import Recipient from "../models/recipient.js";
import { sendWebPushToRecipient, sendSMS } from "../services/notify.js";

import FreqData from "../models/freqData.js";
import { classifyTwoSided } from "../lib/limits.js";
import { brazilPartsFromUTC } from "../lib/time.js";

// aceita 0..2 picos; fs/n opcionais (keep-alive)
const freqSchema = Joi.object({
  device_id: Joi.string().required(),
  ts: Joi.alternatives(Joi.date(), Joi.string(), Joi.number()).optional(),
  status: Joi.string().valid("atividade_detectada","sem_atividade").required(),
  fs: Joi.number().optional(),
  n:  Joi.number().optional(),
  peaks: Joi.array().items(
    Joi.object({ f: Joi.number().required(), mag: Joi.number().required() })
  ).default([]),
  fw: Joi.string().optional()
});

async function notifyBridge(bridge_id, payload, severity) {
  const recipients = await Recipient.find({ bridge_id, active: true, severity });
  for (const r of recipients) {
    if (r.channels?.includes("push")) await sendWebPushToRecipient(r._id, payload);
    if (r.channels?.includes("sms") && r.phone) await sendSMS(r.phone, `${payload.title}: ${payload.body}`);
  }
}

export async function ingestFrequency(req, res, next) {
  try {
    const { value, error } = freqSchema.validate(req.body, { stripUnknown: true });
    if (error) return res.status(400).json({ error: error.message });

    const dev = await Device.findOne({ device_id: value.device_id });
    if (!dev) return res.status(404).json({ error: "Unknown device_id" });

    // timestamps (UTC + BR a partir do ts do ponto)
    const tsUTC = value.ts ? new Date(value.ts) : new Date();
    const { ts_br, date_br, hour_br } = brazilPartsFromUTC(tsUTC);

    // limites
    const lim = await BridgeLimit.findOne({ bridge_id: dev.bridge_id });
    const freqAlert = lim?.freq_alert ?? 3.7;
    const freqCrit  = lim?.freq_critical ?? 7.0;
    const minAlert  = lim?.freq_min_alert ?? null;
    const minCrit   = lim?.freq_min_critical ?? null;

    // severidade: usa o MAIOR pico enviado (0 se vazio)
    const maxPeak = Math.max(0, ...value.peaks.map(p => p.f || 0));
    const sev = value.status === "sem_atividade"
      ? "normal"
      : classifyTwoSided(maxPeak, minAlert, minCrit, freqAlert, freqCrit);

    // grava
    const doc = await FreqData.create({
      company_id: dev.company_id,
      bridge_id:  dev.bridge_id,
      device_id:  value.device_id,
      ts: tsUTC, ts_br, date_br, hour_br,
      status: value.status,
      fs: value.fs,
      n:  value.n,
      peaks: value.peaks,           // <== guarda os DOIS picos (freq + mag)
      fw: value.fw || dev.firmware_version,
      severity: sev,
      limits: {
        freq_alert: freqAlert,
        freq_critical: freqCrit,
        freq_min_alert: minAlert,
        freq_min_critical: minCrit,
        version: lim?.config_version ?? 1
      }
    });

    if (sev !== "normal") {
      const rotulo = sev === "critical" ? "CRÍTICO" : "ALERTA";
      const limiteUsado = sev === "critical" ? (freqCrit ?? freqAlert) : (freqAlert ?? freqCrit);
      const message = `Pico ${maxPeak.toFixed(2)} Hz > limite ${rotulo} (${limiteUsado} Hz)`;

      await Alert.create({
        company_id: dev.company_id,
        bridge_id:  dev.bridge_id,
        device_id:  dev.device_id,
        type: "freq",
        severity: sev,
        message,
        payload: { peaks: value.peaks, limits: doc.limits }
      });

      await notifyBridge(
        dev.bridge_id,
        { title: `Frequência (${rotulo})`, body: message, severity: sev },
        sev
      );
    }

    res.status(201).json({ ok: true });
  } catch (e) { next(e); }
}
