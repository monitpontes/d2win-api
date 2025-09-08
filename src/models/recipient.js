
import mongoose from "mongoose";
const RecipientSchema = new mongoose.Schema(
  {
    company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true },
    bridge_id:  { type: mongoose.Schema.Types.ObjectId, ref: "Bridge", required: true },
    name: { type: String, required: true },
    phone: { type: String },  // +5511999999999
    email: { type: String },
    channels: [{ type: String, enum: ["push","sms","email"] }],
    severity: [{ type: String, enum: ["info","warning","critical"] }],
    active: { type: Boolean, default: true }
  },
  { timestamps: true, collection: "alert_recipients" }
);
RecipientSchema.index({ bridge_id: 1, active: 1 });
export default mongoose.model("Recipient", RecipientSchema);
