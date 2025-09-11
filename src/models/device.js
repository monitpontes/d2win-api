// src/models/device.js
import mongoose from "mongoose";

const DeviceSchema = new mongoose.Schema(
  {
    device_id: { type: String, required: true, unique: true },
    company_id: { type: mongoose.Schema.Types.ObjectId, ref: "Company", required: true },
    bridge_id: { type: mongoose.Schema.Types.ObjectId, ref: "Bridge", required: true },

    // Metadados do dispositivo
    meta: {
      location: { type: String },
      axis: { type: String, enum: ["x", "y", "z"], default: "z" }
    },

    // Tipo/modo de operação
    modo_operacao: { type: String, enum: ["aceleracao", "frequencia", "combo"], default: "aceleracao" },
    
    // Informações do hardware
    model: { type: String },
    position: { type: String },
    channels: [{ type: String }],
    firmware_version: { type: String },
    last_seen: { type: Date },

    // Parâmetros flexíveis (aceita qualquer chave)
    params_current: {
      type: Map,
      of: mongoose.Schema.Types.Mixed,
      default: {}
    },

    // Informações de status
    infos: {
      dados_pendentes: { type: Number, default: 0 },
      sdOK: { type: Boolean, default: true }
    },

    // Controle de reinício remoto
    restartPending: { type: Boolean, default: false },

    // Status ativo
    isActive: { type: Boolean, default: true }
  },
  { timestamps: true, collection: "devices" }
);

DeviceSchema.index({ company_id: 1, bridge_id: 1 });
DeviceSchema.index({ last_seen: -1 });
DeviceSchema.index({ device_id: 1 });

export default mongoose.model("Device", DeviceSchema);

