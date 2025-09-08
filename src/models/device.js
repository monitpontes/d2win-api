
import mongoose from "mongoose";

const DeviceSchema = new mongoose.Schema(
  {
    device_id: { type: String, required: true, unique: true },
    company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true },
    bridge_id:  { type: mongoose.Schema.Types.ObjectId, ref: "Bridge", required: true },

    type: { type: String, enum: ["aceleracao","freq","combo"], default: "aceleracao" },
    model: { type: String },
    position: { type: String },
    channels: [{ type: String }],
    firmware_version: { type: String },
    last_seen: { type: Date },

    // Flexible params (accept any keys)
    params_current: {
      type: Map,
      of: mongoose.Schema.Types.Mixed,
      default: {}
    },

    isActive: { type: Boolean, default: true }
  },
  { timestamps: true, collection: "devices" }
);

DeviceSchema.index({ company_id: 1, bridge_id: 1 });
DeviceSchema.index({ last_seen: -1 });

export default mongoose.model("Device", DeviceSchema);
