// src/controllers/ingestFreq.js
import Joi from "joi";
import mongoose from "mongoose";
import Device from "../models/device.js";
import { toBrazilISOFromUTC } from "../lib/time.js";

// Schema do payload de frequência/FFT
const freqSchema = Joi.object({
  device_id: Joi.string().required(),
  ts: Joi.alternatives(Joi.date(), Joi.string(), Joi.number()).optional(), // timestamp opcional (UTC)
  status: Joi.string().valid("atividade_detectada", "sem_atividade").optional(),
  fs: Joi.number().integer().min(1).optional(), // Hz
  n: Joi.number().integer().min(1).optional(),  // #amostras
  peaks: Joi.array().items(
    Joi.object({ f: Joi.number().required(), mag: Joi.number().required() })
  ).default([]),
  fw: Joi.string().optional()
});

export async function ingestFrequency(req, res, next) {
  try {
    // 1) Validação
    const { value: body, error } = freqSchema.validate(req.body, {
      stripUnknown: true
    });
    if (error) return res.status(400).json({ error: error.message });

    // 2) Dispositivo
    const dev = await Device.findOne({ device_id: body.device_id });
    if (!dev) return res.status(404).json({ error: "Unknown device_id" });

    // 3) Tempo (UTC + Brasil)
    const tsUTC = body.ts ? new Date(body.ts) : new Date();
    const ts_br = toBrazilISOFromUTC(tsUTC);
    const date_br = ts_br.slice(0, 10);
    const hour_br = Number(ts_br.slice(11, 13));

    // 4) Persistência APENAS no time-series
    await mongoose.connection.db
      .collection("telemetry_ts_freq_peaks")
      .insertOne({
        meta: {
          company_id: dev.company_id,
          bridge_id:  dev.bridge_id,
          device_id:  dev.device_id
        },
        ts: tsUTC,     // <- timestamp UTC (Date)
        ts_br,         // <- timestamp no fuso de Brasília (string)
        date_br,
        hour_br,
        status: body.status || "atividade_detectada", // ou "sem_atividade"
        fs: body.fs ?? null,
        n: body.n ?? null,
        peaks: body.peaks, // [{ f, mag }, ...]
        fw: body.fw || dev.firmware_version
      });

    return res.status(201).json({ ok: true });
  } catch (e) {
    next(e);
  }
}
