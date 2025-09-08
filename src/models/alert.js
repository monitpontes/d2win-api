
import mongoose from "mongoose";
const AlertSchema = new mongoose.Schema(
  {
    company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true },
    bridge_id:  { type: mongoose.Schema.Types.ObjectId, ref: "Bridge", required: true },
    device_id:  { type: String, required: true },
    type: { type: String, enum: ["accel","freq"], required: true },
    severity: { type: String, enum: ["info","warning","critical"], default: "warning" },
    message: { type: String, required: true },
    ts: { type: Date, default: Date.now },
    payload: { type: Object, default: {} },
    status: { type: String, enum: ["open","ack","closed"], default: "open" }
  },
  { timestamps: true, collection: "alerts" }
);
AlertSchema.index({ bridge_id: 1, ts: -1 });
AlertSchema.index({ device_id: 1, ts: -1 });
export default mongoose.model("Alert", AlertSchema);
