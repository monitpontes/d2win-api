import mongoose from "mongoose";
import Company from "../models/company.js"; // ajuste o caminho se precisar

function computeAbbr(name) {
  if (!name) return "";
  const clean = String(name)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^A-Za-z0-9\s]/g, "")
    .trim();
  if (!clean) return "";
  const parts = clean.split(/\s+/).filter(Boolean);
  if (parts.length === 1) return parts[0].toUpperCase().slice(0, 8);
  return parts.map((p) => p[0]).join("").toUpperCase().slice(0, 8);
}

(async () => {
  await mongoose.connect(process.env.MONGO_URI);
  const companies = await Company.find({ $or: [{ abbr: { $exists: false } }, { abbr: "" }] });
  for (const c of companies) {
    c.abbr = computeAbbr(c.name);
    await c.save();
    console.log(`Atualizada: ${c.name} -> ${c.abbr}`);
  }
  await mongoose.disconnect();
  process.exit(0);
})();
