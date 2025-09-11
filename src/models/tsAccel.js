// src/models/tsAccel.js
import mongoose from "mongoose";

const MetaSchema = new mongoose.Schema(
  {
    object_id: { type: mongoose.Schema.Types.ObjectId, required: true },
    company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true },
    bridge_id:  { type: mongoose.Schema.Types.ObjectId, ref: "Bridge", required: true },
    device_id:  { type: String, required: true },
    axis:       { type: String, enum: ["x", "y", "z"], default: "z" },
  },
  { _id: false }
);

const TsAccelSchema = new mongoose.Schema(
  {
    device_id: { type: String, required: true },
    meta:      { type: MetaSchema, required: true },
    ts:        { type: Date, required: true }, // UTC
    ts_br:     { type: String, required: true },
    value:     { type: Number, required: true }, // m/s²
    severity:  { type: String, enum: ["normal", "warning", "critical"], default: "normal", index: true },
  },
  { collection: "telemetry_ts_accel", versionKey: false }
);

export default mongoose.model("TsAccel", TsAccelSchema);
