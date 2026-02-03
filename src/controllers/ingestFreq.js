// src/controllers/ingestFreq.js
import Joi from "joi";
import mongoose from "mongoose";
import Device from "../models/device.js";
import BridgeLimit from "../models/bridgeLimit.js";
import TsFreqPeaks from "../models/tsFreq.js";
import { toBrazilISOFromUTC } from "../lib/time.js";

const freqSchema = Joi.object({
  device_id: Joi.string().required(),
  ts:     Joi.alternatives(Joi.date(), Joi.string(), Joi.number()).optional(),
  status: Joi.string().valid("atividade_detectada", "sem_atividade").optional(),
  fs:     Joi.number().integer().min(1).optional(),
  n:      Joi.number().integer().min(1).optional(),
  peaks:  Joi.array().items(Joi.object({ f: Joi.number().required(), mag: Joi.number().required() })).default([])
});

// ---- Limites para FREQ (por dispositivo com fallback p/ BridgeLimit)
// Aceita vários nomes comuns para facilitar migração.
function resolveFreqLimitsFromDevice(dev, bridgeLim) {
  const fl =
    dev?.limits?.freq ||
    dev?.params_current?.limits?.freq ||
    {};

  return {
    f_warning:  fl.f_warning  ?? fl.warning      ?? bridgeLim?.freq_alert ?? null,
    f_critical: fl.f_critical ?? fl.critical     ?? bridgeLim?.freq_critical ?? null
  };
}

function classifyFreqSeverity(status, peaks, lim) {
  // Se não tiver picos, use o status como fallback
  if (!Array.isArray(peaks) || peaks.length === 0) {
    return status === "atividade_detectada" ? "warning" : "normal";
  }

  // Escolhe o pico principal: o de maior magnitude (mais “forte”)
  const mainPeak = peaks.reduce((best, p) => {
    const mag = Math.abs(Number(p.mag) || 0);
    const bestMag = Math.abs(Number(best?.mag) || 0);
    return mag > bestMag ? p : best;
  }, peaks[0]);

  const f = Math.abs(Number(mainPeak.f) || 0);

  // Sem limites definidos → comportamento conservador
  if (lim.f_warning == null && lim.f_critical == null) {
    return status === "atividade_detectada" ? "warning" : "normal";
  }

  // ⚠️ Importante: sua regra de “crítico” depende do que você quer dizer com limite:
  // Se "freq_critical" é um "limite máximo" (f >= critical), use assim:
  if (lim.f_critical != null && f >= lim.f_critical) return "critical";
  if (lim.f_warning  != null && f >= lim.f_warning)  return "warning";

  return "normal";
}

export async function ingestFrequency(req, res, next) {
  try {
    const { value: body, error } = freqSchema.validate(req.body, { stripUnknown: true });
    if (error) return res.status(400).json({ error: error.message });

    const dev = await Device.findOne({ device_id: body.device_id });
    if (!dev) return res.status(404).json({ error: "Unknown device_id" });

    const tsUTC = body.ts ? new Date(body.ts) : new Date();
    const ts_br = toBrazilISOFromUTC(tsUTC);

    const bridgeLim = await BridgeLimit.findOne({ bridge_id: dev.bridge_id });
    const lim = resolveFreqLimitsFromDevice(dev, bridgeLim);
    const severity = classifyFreqSeverity(body.status || "atividade_detectada", body.peaks, lim);

    const docId = new mongoose.Types.ObjectId();
    await TsFreqPeaks.create({
      _id: docId,
      device_id: body.device_id,
      meta: {
        object_id: docId,
        company_id: dev.company_id,
        bridge_id:  dev.bridge_id,
        device_id:  dev.device_id
      },
      ts: tsUTC,
      ts_br,
      status: body.status || "atividade_detectada",
      fs: body.fs ?? null,
      n:  body.n  ?? null,
      peaks: body.peaks,
      severity
    });

    await Device.updateOne(
  { device_id: body.device_id },
  { $set: { last_seen: tsUTC } }
);

    return res.status(201).json({ ok: true, id: docId.toString() });
  } catch (e) {
    next(e);
  }
}
