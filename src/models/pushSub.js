
import mongoose from "mongoose";
const PushSubSchema = new mongoose.Schema(
  {
    bridge_id:  { type: mongoose.Schema.Types.ObjectId, ref: "Bridge" },
    company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company" },
    recipient_id: { type: mongoose.Schema.Types.ObjectId, ref: "Recipient" },
    endpoint: { type: String, required: true, unique: true },
    keys: { p256dh: String, auth: String }
  },
  { timestamps: true, collection: "push_subscriptions" }
);
PushSubSchema.index({ recipient_id: 1 });
export default mongoose.model("PushSub", PushSubSchema);
