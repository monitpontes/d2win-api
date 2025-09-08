
import mongoose from "mongoose";
const BridgeSchema = new mongoose.Schema(
  {
    company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true }, // owner
    name: { type: String, required: true }, // like "Ponte do Jaguaré"
    code: { type: String }, // like "SP-348"
    location: { lat: Number, lng: Number }, 
    tags: [String], 
    isActive: { type: Boolean, default: true }
  },
  { timestamps: true, collection: "bridges" }
);
BridgeSchema.index({ company_id: 1, name: 1 });
export default mongoose.model("Bridge", BridgeSchema);
