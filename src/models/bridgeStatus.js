// src/models/bridgeStatus.js
import mongoose from "mongoose";

const DeviceMiniSchema = new mongoose.Schema({
  device_id:  { type: String, required: true, index: true },
  last_seen:  { type: Date },              // UTC
  ts_br:      { type: String },            // "YYYY-MM-DDTHH:mm:ss.SSS" (BR-3, sem 'Z')
  ms_since:   { type: Number, default: null },
  status:     { type: String, enum: ["active","stale","offline"], default: "offline" },
  rssi:       { type: Number },
  battery_v:  { type: Number },
}, { _id: false });

const BridgeStatusSchema = new mongoose.Schema({
  company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true, index: true },
  bridge_id:  { type: mongoose.Schema.Types.ObjectId, ref: "Bridge", required: true, index: true },

  // metadados com nomes legíveis
  meta: {
    company_name: { type: String },
    bridge_name:  { type: String }
  },

  // resumo + marcação de tempo
  updated_at: { type: Date, default: () => new Date() }, // UTC
  ts_br:      { type: String },                           // BR-3
  date_br:    { type: String },                           // "YYYY-MM-DD"
  hour_br:    { type: Number },                           // 0..23

  summary: {
    total:    { type: Number, default: 0 },
    active:   { type: Number, default: 0 },
    stale:    { type: Number, default: 0 },
    offline:  { type: Number, default: 0 },
    status:   { type: String, enum: ["active","stale","offline"], default: "offline" }, // status da ponte
  },

  // "snapshot" dos dispositivos dessa ponte
  devices: { type: [DeviceMiniSchema], default: [] },
}, { collection: "bridge_device_status", versionKey: false });

// um documento por ponte
BridgeStatusSchema.index({ company_id: 1, bridge_id: 1 }, { unique: true });

export default mongoose.model("BridgeStatus", BridgeStatusSchema);
