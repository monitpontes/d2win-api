// src/models/company.js
import mongoose from "mongoose";
const CompanySchema = new mongoose.Schema(
  { name: { type: String, required: true }, isActive: { type: Boolean, default: true } },
  { timestamps: true, collection: "companies" }
);
export default mongoose.model("Company", CompanySchema);

// import mongoose from "mongoose";

// const CompanySchema = new mongoose.Schema(
//   {
//     // Campos existentes
//     name: { type: String, required: true },
//     isActive: { type: Boolean, default: true },
    
//     // === NOVOS CAMPOS ===
    
//     // Identificação
//     description: { type: String },
//     logo: { type: String },  // URL do logo
//     cnpj: { type: String },
    
//     // Contato da empresa
//     email: { type: String },
//     phone: { type: String },
    
//     // Endereço
//     address: { type: String },
//     city: { type: String },
//     state: { type: String },
//     zip_code: { type: String },
    
//     // Contato responsável
//     contact_name: { type: String },
//     contact_email: { type: String },
//     contact_phone: { type: String },
//   },
//   { timestamps: true, collection: "companies" }
// );

// export default mongoose.model("Company", CompanySchema);
