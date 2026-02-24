// import mongoose from "mongoose";
// const BridgeSchema = new mongoose.Schema(
//   {
//     company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true }, // owner
//     name: { type: String, required: true }, // like "Ponte do Jaguaré"
//     code: { type: String }, // like "SP-348"
//     location: { lat: Number, lng: Number }, 
//     tags: [String], 
//     isActive: { type: Boolean, default: true }
//   },
//   { timestamps: true, collection: "bridges" }
// );
// BridgeSchema.index({ company_id: 1, name: 1 });
// export default mongoose.model("Bridge", BridgeSchema);

// src/models/bridge.js
import mongoose from "mongoose";

/* ----------------------------- Subschemas ----------------------------- */

const GeoJSONSchema = new mongoose.Schema(
  {
    type: {
      type: String,
      enum: ["FeatureCollection"],
      default: "FeatureCollection",
    },
    // Features: [{ type:"Feature", geometry:{...}, properties:{...} }, ...]
    features: { type: Array, default: [] },
  },
  { _id: false }
);

// Anotações 3D do Editor (labels/textos no modelo)
const Annotation3DSchema = new mongoose.Schema(
  {
    // opcional: id gerado no frontend (uuid) para facilitar update/remove sem depender de índice
    id: { type: String },

    text: { type: String, default: "" },

    position: { type: [Number], default: [0, 0, 0] },

    // opcional (se o frontend quiser estilizar depois)
    color: { type: String },
  },
  { _id: false }
);

/* ------------------------------ Bridge ------------------------------ */

const BridgeSchema = new mongoose.Schema(
  {
    // ===== Owner =====
    company_id: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Company",
      required: true,
      index: true,
    },

    // ===== Básico =====
    name: { type: String, required: true }, // "Ponte do Jaguaré"
    code: { type: String }, // "SP-348"
    tags: { type: [String], default: [] },
    isActive: { type: Boolean, default: true },

    // ===== Localização =====
    locationText: { type: String }, // "São Paulo - SP" (texto descritivo)
    coordinates: {
      lat: { type: Number },
      lng: { type: Number },
    },

    // ===== Operação / Contexto (editável) =====
    // (não é status dos sensores; isso será "computed" pela API)
    criticality: {
      type: String,
      enum: ["low", "medium", "high"],
      default: "low",
    },
    impactNotes: { type: String }, // texto livre, opcional

    // ===== Concessão / Rodovia =====
    concession: { type: String }, // Ex: "CCR AutoBAn"
    rodovia: { type: String }, // Ex: "SP-348"
    km: { type: String }, // Ex: 83.4

    // ===== Tipologia / Estrutura =====
    typology: {
      type: String,
      enum: ["Ponte", "Viaduto", "Passarela"],
      default: "Ponte",
    },
    beamType: { type: String }, // Ex: "Caixão", "I", "T"
    spanType: { type: String }, // Ex: "Simples", "Contínuo"
    material: { type: String }, // Ex: "Concreto Armado", "Aço", "Misto"
    supportCount: { type: Number }, // Nº de apoios/pilares

    // ===== Dimensões =====
    length: { type: Number }, // m
    width: { type: Number }, // m
    capacity: { type: Number }, // t (se usar)

    // ===== Histórico =====
    constructionYear: { type: Number },
    lastMajorIntervention: { type: String },

    // ===== Mídia =====
    image: { type: String }, // URL/arquivo futuro
    geoReferencedImage: { type: String }, // URL/arquivo futuro

    // ===== Editor 3D (Admin) =====
    // Anotações de texto posicionadas no modelo 3D (labels)
    annotations3d: { type: [Annotation3DSchema], default: [] },

    // ===== Mapa =====
    // GeoJSON para desenhar no mapa (não precisa ser indexado agora)
    geojson: { type: GeoJSONSchema, default: undefined },
  },
  { timestamps: true, collection: "bridges" }
);

// Indexes
BridgeSchema.index({ company_id: 1, name: 1 });

// opcional: facilita filtros básicos por coordenadas (não é 2dsphere)
BridgeSchema.index({ "coordinates.lng": 1, "coordinates.lat": 1 });

export default mongoose.model("Bridge", BridgeSchema);