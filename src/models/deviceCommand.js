
import mongoose from "mongoose";
const DeviceCommandSchema = new mongoose.Schema(
  {
    device_id: { type: String, required: true },
    type: { type: String, enum: ["restart","set_params"], required: true },
    payload: { type: Object, default: {} },
    status: { type: String, enum: ["pending","sent","acked"], default: "pending" },
    issued_at: { type: Date, default: Date.now },
    acked_at: { type: Date }
  },
  { collection: "device_commands" }
);
DeviceCommandSchema.index({ device_id: 1, issued_at: -1 });
export default mongoose.model("DeviceCommand", DeviceCommandSchema);
