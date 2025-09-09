// models/freqData.js
import mongoose from "mongoose";

const FreqSchema = new mongoose.Schema({
  company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true },
  bridge_id:  { type: mongoose.Schema.Types.ObjectId, ref: "Bridge", required: true },
  device_id:  { type: String, required: true },

  ts:     { type: Date, required: true, index: true },
  ts_br:  { type: String },
  date_br:{ type: String, index: true },
  hour_br:{ type: Number, index: true },

  status: { type: String, enum: ["atividade_detectada","sem_atividade"], required: true },
  fs:     { type: Number }, 
  n:      { type: Number }, 
  peaks:  [{ f: Number, mag: Number }],
  fw:     { type: String },

  severity: { type: String, enum: ["normal","warning","critical"], default: "normal", index: true },

  limits: {
    freq_alert:    { type: Number },
    freq_critical: { type: Number },
    freq_min_alert:    { type: Number },
    freq_min_critical: { type: Number },
    version: { type: Number }
  }
}, { timestamps: true, collection: "freq_data" });

export default mongoose.model("FreqData", FreqSchema);
