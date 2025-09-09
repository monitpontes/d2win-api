// models/accelData.js
import mongoose from "mongoose";

const AccelSchema = new mongoose.Schema({
  company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true },
  bridge_id:  { type: mongoose.Schema.Types.ObjectId, ref: "Bridge", required: true },
  device_id:  { type: String, required: true },

  ts:     { type: Date, required: true, index: true }, // UTC
  ts_br:  { type: String }, 
  date_br:{ type: String, index: true }, 
  hour_br:{ type: Number, index: true }, 

  axis:   { type: String, enum: ["x","y","z"], default: "z" },
  value:  { type: Number, required: true },
  fw:     { type: String },

  severity: { type: String, enum: ["normal","warning","critical"], default: "normal", index: true },

  // snapshot dos limites usados na classificação
  limits: {
    accel_alert:    { type: Number },
    accel_critical: { type: Number },
    accel_min_alert:    { type: Number },
    accel_min_critical: { type: Number },
    version: { type: Number }
  }
}, { timestamps: true, collection: "accel_data" });

export default mongoose.model("AccelData", AccelSchema);
