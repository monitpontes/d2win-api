import mongoose from "mongoose";

const UserSchema = new mongoose.Schema(
  {
    name: { type: String, required: true },
    email: { type: String, required: true, unique: true, index: true },
    // NÃO use select:false neste momento, pois você compara a senha no login
    passwordHash: { type: String, required: true },
    role: { type: String, enum: ["viewer", "gestor", "admin"], default: "viewer" },
    company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company" },
    isActive: { type: Boolean, default: true },
    lastLogin: { type: Date, default: null },
  },
  { timestamps: true, collection: "users" }
);

export default mongoose.model("User", UserSchema);
