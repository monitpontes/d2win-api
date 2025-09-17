// src/models/company.js
import mongoose from "mongoose";
const CompanySchema = new mongoose.Schema(
  { name: { type: String, required: true }, isActive: { type: Boolean, default: true } },
  { timestamps: true, collection: "companies" }
);
export default mongoose.model("Company", CompanySchema);
