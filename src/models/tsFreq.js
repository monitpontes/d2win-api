// src/models/tsFreqPeaks.js
import mongoose from "mongoose";

const MetaSchema = new mongoose.Schema(
  {
    object_id: { type: mongoose.Schema.Types.ObjectId, required: true },
    company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true },
    bridge_id:  { type: mongoose.Schema.Types.ObjectId, ref: "Bridge", required: true },
    device_id:  { type: String, required: true },
    stream:     { type: String, default: "freq:z" },
  },
  { _id: false }
);

const PeakSchema = new mongoose.Schema(
  { f: { type: Number, required: true }, mag: { type: Number, required: true } },
  { _id: false }
);

const TsFreqPeaksSchema = new mongoose.Schema(
  {
    device_id: { type: String, required: true },
    meta:      { type: MetaSchema, required: true },
    ts:        { type: Date, required: true }, // UTC
    ts_br:     { type: String, required: true },
    status:    { type: String, enum: ["atividade_detectada", "sem_atividade"], default: "atividade_detectada" },
    fs:        { type: Number },
    n:         { type: Number },
    peaks:     { type: [PeakSchema], default: [] },
    severity:  { type: String, enum: ["normal", "warning", "critical"], default: "normal", index: true },
  },
  { collection: "telemetry_ts_freq_peaks", versionKey: false }
);

// índices úteis para dashboards/consultas
TsFreqPeaksSchema.index({ "meta.company_id": 1, "meta.bridge_id": 1, "meta.device_id": 1, ts: -1 });
TsFreqPeaksSchema.index({ "meta.device_id": 1, ts: -1 });
TsFreqPeaksSchema.index({ ts: -1 });

export default mongoose.model("TsFreqPeaks", TsFreqPeaksSchema);
