
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


// import mongoose from "mongoose";

// const BridgeSchema = new mongoose.Schema(
//   {
//     // Campos existentes
//     company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true },
//     name: { type: String, required: true },
//     code: { type: String },
//     tags: [String],
//     isActive: { type: Boolean, default: true },
    
//     // === NOVOS CAMPOS ===
    
//     // Localização e identificação
//     location: { type: String },  // Texto descritivo (ex: "São Paulo - SP")
//     coordinates: {
//       lat: { type: Number },
//       lng: { type: Number }
//     },
//     concession: { type: String },  // Ex: "CCR AutoBAn"
//     rodovia: { type: String },     // Ex: "SP-348"
//     km: { type: Number },          // Quilometragem
    
//     // Tipologia e estrutura
//     typology: { 
//       type: String, 
//       enum: ['Ponte', 'Viaduto', 'Passarela'],
//       default: 'Ponte'
//     },
//     beamType: { type: String },    // Ex: "Caixão", "I", "T"
//     spanType: { type: String },    // Ex: "Simples", "Contínuo", "Gerber"
//     material: { type: String },    // Ex: "Concreto Armado", "Aço", "Misto"
//     supportCount: { type: Number }, // Número de apoios/pilares
    
//     // Dimensões
//     length: { type: Number },       // Comprimento em metros
//     width: { type: Number },        // Largura em metros
//     capacity: { type: Number },     // Capacidade de carga (toneladas)
    
//     // Histórico
//     constructionYear: { type: Number },
//     lastMajorIntervention: { type: String },
    
//     // Status operacional
//     structuralStatus: { 
//       type: String, 
//       enum: ['operacional', 'atencao', 'restricoes', 'critico', 'interdicao'],
//       default: 'operacional'
//     },
//     operationalCriticality: { 
//       type: String, 
//       enum: ['low', 'medium', 'high'],
//       default: 'low'
//     },
//     operationalImpact: { type: String },
    
//     // Monitoramento
//     sensorCount: { type: Number, default: 0 },
//     hasActiveAlerts: { type: Boolean, default: false },
//     lastUpdate: { type: Date },
    
//     // Mídia
//     image: { type: String },           // URL da imagem principal
//     geoReferencedImage: { type: String }, // URL da imagem georreferenciada
//     kmz_file: { type: String },        // URL do arquivo KMZ
//   },
//   { timestamps: true, collection: "bridges" }
// );

// BridgeSchema.index({ company_id: 1, name: 1 });

// export default mongoose.model("Bridge", BridgeSchema);
