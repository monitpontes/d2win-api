// src/routes/bridges.js
import express from "express";
import Bridge from "../models/bridge.js";
import Company from "../models/company.js";
import Device from "../models/device.js"; // <— ADICIONE ISSO

const router = express.Router();

/** ===== Helpers para gerar IDs sem precisar de abbr persistido ===== **/
function sanitizeNameToAbbr(name, max = 12) {
  if (!name) return "";
  const clean = String(name)
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "") // tira acento
    .replace(/[^A-Za-z0-9\s]/g, " ")                 // tira símbolos
    .replace(/\s+/g, " ")                            // normaliza espaços
    .trim();
  if (!clean) return "";
  const parts = clean.split(" ").filter(Boolean);
  if (parts.length === 1) return parts[0].toUpperCase().slice(0, max);
  // várias palavras → iniciais (ex.: "Ponte Rio-Niterói" => "PRN")
  return parts.map(p => p[0]).join("").toUpperCase().slice(0, max);
}
function pad2(n) { return String(n).padStart(2, "0"); }

/**
 * Cria devices para a ponte recém-criada sem duplicar.
 * Formato: EMP_ABBR_BRIDGE_ABBR_S01..SNN (ex.: MOTIVA_OAE2_S01)
 */
async function createDevicesForBridge({ company, bridge, sensorsCount = 5 }) {
  const compAbbr   = sanitizeNameToAbbr(company.name, 12);
  const bridgeAbbr = sanitizeNameToAbbr(bridge.name, 16);
  const prefix = `${compAbbr}_${bridgeAbbr}_S`;

  // Descobre quais já existem com o mesmo prefixo (para não duplicar)
  const existing = await Device.find({
    isActive: true,
    company_id: company._id,
    bridge_id: bridge._id,
    device_id: { $regex: `^${prefix}\\d{2}$`, $options: "i" },
  }).select("device_id");

  const existingSet = new Set(existing.map(d => d.device_id.toUpperCase()));

  const docs = [];
  for (let i = 1; i <= sensorsCount; i++) {
    const id = `${prefix}${pad2(i)}`; // MOTIVA_OAE2_S01
    if (existingSet.has(id.toUpperCase())) continue;

    // Defaults alinhados aos nomes da sua API
    docs.push({
      device_id: id,
      company_id: company._id,
      bridge_id: bridge._id,
      isActive: true,
      last_seen: null,
      meta: { location: bridge.location || "", axis: "z" },

      // valores iniciais compatíveis com seu payload (pode ajustar)
      params_current: {
        modo_teste: "completo",
        tempo_calibracao: 5000,
        intervalo_aquisicao: 1000,
        amostras: 4096,
        freq_amostragem: 2000,
        activity_threshold: 1,
        modo_execucao: "debug",        // "debug" | "operacao" | "teste"
      },
      infos: {
        dados_pendentes: 0,
        sDOK: true,
        restartPending: false,
        firmware_version: "3.4.0_autoregister",
        modo_operacao: "aceleracao",   // "aceleracao" | "frequencia"
      },
    });
  }

  if (docs.length) {
    await Device.insertMany(docs, { ordered: false });
  }
}

/** ========================= ROTAS ========================= **/

// GET /bridges
router.get("/", async (req, res) => {
  try {
    const { company_id } = req.query;
    const filter = { isActive: true };
    if (company_id) filter.company_id = company_id;

    const bridges = await Bridge.find(filter)
      .populate("company_id", "name")
      .sort({ name: 1 })
      .lean();

    const bridgesWithNames = bridges.map(b => ({
      ...b,
      company_name: b.company_id?.name || "Empresa não encontrada",
    }));

    res.json(bridgesWithNames);
  } catch (error) {
    console.error("Error fetching bridges:", error);
    res.status(500).json({ message: "Erro ao buscar pontes", error: error.message });
  }
});

// GET /bridges/:id
router.get("/:id", async (req, res) => {
  try {
    const bridge = await Bridge.findById(req.params.id)
      .populate("company_id", "name")
      .lean();

    if (!bridge) return res.status(404).json({ message: "Ponte não encontrada" });

    res.json({
      ...bridge,
      company_name: bridge.company_id?.name || "Empresa não encontrada",
    });
  } catch (error) {
    console.error("Error fetching bridge:", error);
    res.status(500).json({ message: "Erro ao buscar ponte", error: error.message });
  }
});

// POST /bridges  (cria ponte + devices automáticos)
router.post("/", async (req, res) => {
  try {
    const { name, company_id, location, description, sensors_count } = req.body;

    if (!name?.trim())   return res.status(400).json({ message: "Nome da ponte é obrigatório" });
    if (!company_id)     return res.status(400).json({ message: "ID da empresa é obrigatório" });

    const company = await Company.findById(company_id);
    if (!company || !company.isActive) {
      return res.status(404).json({ message: "Empresa não encontrada" });
    }

    const existingBridge = await Bridge.findOne({
      name: { $regex: new RegExp(`^${name.trim()}$`, "i") },
      company_id,
      isActive: true,
    });
    if (existingBridge) {
      return res.status(409).json({ message: "Já existe uma ponte com este nome nesta empresa" });
    }

    const bridge = new Bridge({
      name: name.trim(),
      company_id,
      location: location?.trim() || "",
      description: description?.trim() || "",
      isActive: true,
    });

    const savedBridge = await bridge.save();

    // CRIA DEVICES (não falha a criação da ponte se der erro)
    try {
      const count = Number(sensors_count) > 0 ? Number(sensors_count) : 5;
      await createDevicesForBridge({ company, bridge: savedBridge, sensorsCount: count });
    } catch (e) {
      console.warn("Falha ao criar devices automáticos:", e?.message);
    }

    const populatedBridge = await Bridge.findById(savedBridge._id)
      .populate("company_id", "name")
      .lean();

    res.status(201).json({
      ...populatedBridge,
      company_name: populatedBridge.company_id?.name || "Empresa não encontrada",
    });
  } catch (error) {
    console.error("Error creating bridge:", error);
    res.status(500).json({ message: "Erro ao criar ponte", error: error.message });
  }
});

// PUT /bridges/:id
router.put("/:id", async (req, res) => {
  try {
    const { name, company_id, location, description } = req.body;

    if (!name?.trim())   return res.status(400).json({ message: "Nome da ponte é obrigatório" });
    if (!company_id)     return res.status(400).json({ message: "ID da empresa é obrigatório" });

    const company = await Company.findById(company_id);
    if (!company || !company.isActive) {
      return res.status(404).json({ message: "Empresa não encontrada" });
    }

    const existingBridge = await Bridge.findOne({
      _id: { $ne: req.params.id },
      name: { $regex: new RegExp(`^${name.trim()}$`, "i") },
      company_id,
      isActive: true,
    });
    if (existingBridge) {
      return res.status(409).json({ message: "Já existe uma ponte com este nome nesta empresa" });
    }

    const bridge = await Bridge.findByIdAndUpdate(
      req.params.id,
      {
        name: name.trim(),
        company_id,
        location: location?.trim() || "",
        description: description?.trim() || "",
        updatedAt: new Date(),
      },
      { new: true, runValidators: true }
    ).populate("company_id", "name");

    if (!bridge) return res.status(404).json({ message: "Ponte não encontrada" });

    res.json({
      ...bridge.toObject(),
      company_name: bridge.company_id?.name || "Empresa não encontrada",
    });
  } catch (error) {
    console.error("Error updating bridge:", error);
    res.status(500).json({ message: "Erro ao atualizar ponte", error: error.message });
  }
});

// DELETE /bridges/:id  (soft delete)  + (opcional) cascade nos devices
router.delete("/:id", async (req, res) => {
  try {
    const bridge = await Bridge.findByIdAndUpdate(
      req.params.id,
      { isActive: false, updatedAt: new Date() },
      { new: true }
    );
    if (!bridge) return res.status(404).json({ message: "Ponte não encontrada" });

    // (opcional) também desativar devices vinculados
    try {
      await Device.updateMany(
        { bridge_id: bridge._id, isActive: true },
        { $set: { isActive: false, "infos.modo_operacao": "aceleracao" }, $currentDate: { updatedAt: true } }
      );
    } catch (e) {
      console.warn("Falha ao desativar devices da ponte:", e?.message);
    }

    res.json({ message: "Ponte removida com sucesso" });
  } catch (error) {
    console.error("Error deleting bridge:", error);
    res.status(500).json({ message: "Erro ao remover ponte", error: error.message });
  }
});

export default router;