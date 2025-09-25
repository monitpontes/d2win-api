
import mongoose from "mongoose";

const BridgeLimitSchema = new mongoose.Schema(
  {
    company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true },
    bridge_id:  { type: mongoose.Schema.Types.ObjectId, ref: "Bridge", required: true },
    accel_alert: { type: Number, default: 11.0 },
    accel_critical: { type: Number, default: 12.0 },
    freq_alert: { type: Number, default: 3.7 },
    freq_critical: { type: Number, default: 7.0 }
  },
  { timestamps: true, collection: "bridge_limits" }
);
BridgeLimitSchema.index({ company_id: 1, bridge_id: 1 }, { unique: true });

export default mongoose.model("BridgeLimit", BridgeLimitSchema);
