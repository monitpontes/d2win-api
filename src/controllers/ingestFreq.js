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
    mag_warning:  fl.mag_warning  ?? fl.warning      ?? fl.peak_warning ??
                  bridgeLim?.freq_mag_alert ?? bridgeLim?.freq_alert ?? null,
    mag_critical: fl.mag_critical ?? fl.critical     ?? fl.peak_critical ??
                  bridgeLim?.freq_mag_critical ?? bridgeLim?.freq_critical ?? null
  };
}

function classifyFreqSeverity(status, peaks, lim) {
  if (!Array.isArray(peaks) || peaks.length === 0) {
    // sem picos: keep-alive -> normal; atividade sem picos -> warning (conservador)
    return status === "atividade_detectada" ? "warning" : "normal";
  }
  const maxMag = Math.max(...peaks.map(p => Math.abs(Number(p.mag) || 0)));
  if (lim.mag_critical != null && maxMag >= lim.mag_critical) return "critical";
  if (lim.mag_warning  != null && maxMag >= lim.mag_warning)  return "warning";
  // se não há limites definidos, reporta warning quando há atividade
  if (lim.mag_warning == null && lim.mag_critical == null) {
    return status === "atividade_detectada" ? "warning" : "normal";
  }
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

    return res.status(201).json({ ok: true, id: docId.toString() });
  } catch (e) {
    next(e);
  }
}
