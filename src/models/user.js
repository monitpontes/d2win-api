// src/models/user.js
const mongoose = require("mongoose")

const UserSchema = new mongoose.Schema(
  {
    name: { type: String, required: true },
    email: { type: String, required: true, unique: true, index: true },
    passwordHash: { type: String, required: true },
    role: { type: String, enum: ["viewer", "gestor", "admin"], default: "viewer" },
    isActive: { type: Boolean, default: true },
    company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company" },
    lastLogin: { type: Date },
  },
  { timestamps: true }
)

module.exports = mongoose.model("User", UserSchema)
