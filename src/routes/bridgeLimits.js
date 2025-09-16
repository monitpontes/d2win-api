// src/routes/bridgeLimits.js
import express from "express";
import BridgeLimit from "../models/bridgeLimit.js";
import Company from "../models/company.js";
import Bridge from "../models/bridge.js";

const router = express.Router();

// GET /bridge-limits - Listar todos os limites
router.get("/", async (req, res) => {
  try {
    const { company_id, bridge_id } = req.query;
    
    const filter = {};
    if (company_id) filter.company_id = company_id;
    if (bridge_id) filter.bridge_id = bridge_id;
    
    const limits = await BridgeLimit.find(filter)
      .populate('company_id', 'name')
      .populate('bridge_id', 'name')
      .sort({ updatedAt: -1 })
      .lean();
    
    // Adicionar nomes para facilitar uso no frontend
    const limitsWithNames = limits.map(limit => ({
      ...limit,
      company_name: limit.company_id?.name || 'Empresa não encontrada',
      bridge_name: limit.bridge_id?.name || 'Ponte não encontrada'
    }));
    
    res.json(limitsWithNames);
  } catch (error) {
    console.error("Error fetching bridge limits:", error);
    res.status(500).json({ message: "Erro ao buscar limites das pontes", error: error.message });
  }
});

// GET /bridge-limits/:id - Buscar limite por ID
router.get("/:id", async (req, res) => {
  try {
    const limit = await BridgeLimit.findById(req.params.id)
      .populate('company_id', 'name')
      .populate('bridge_id', 'name')
      .lean();
    
    if (!limit) {
      return res.status(404).json({ message: "Limite não encontrado" });
    }
    
    const limitWithNames = {
      ...limit,
      company_name: limit.company_id?.name || 'Empresa não encontrada',
      bridge_name: limit.bridge_id?.name || 'Ponte não encontrada'
    };
    
    res.json(limitWithNames);
  } catch (error) {
    console.error("Error fetching bridge limit:", error);
    res.status(500).json({ message: "Erro ao buscar limite", error: error.message });
  }
});

// POST /bridge-limits - Criar novo limite
router.post("/", async (req, res) => {
  try {
    const { company_id, bridge_id, accel_alert, accel_critical, freq_alert, freq_critical } = req.body;
    
    // Validações
    if (!company_id) {
      return res.status(400).json({ message: "ID da empresa é obrigatório" });
    }
    
    if (!bridge_id) {
      return res.status(400).json({ message: "ID da ponte é obrigatório" });
    }
    
    if (typeof accel_alert !== 'number' || accel_alert < 0) {
      return res.status(400).json({ message: "Limite de alerta de aceleração deve ser um número positivo" });
    }
    
    if (typeof accel_critical !== 'number' || accel_critical < 0) {
      return res.status(400).json({ message: "Limite crítico de aceleração deve ser um número positivo" });
    }
    
    if (typeof freq_alert !== 'number' || freq_alert < 0) {
      return res.status(400).json({ message: "Limite de alerta de frequência deve ser um número positivo" });
    }
    
    if (typeof freq_critical !== 'number' || freq_critical < 0) {
      return res.status(400).json({ message: "Limite crítico de frequência deve ser um número positivo" });
    }
    
    if (accel_critical <= accel_alert) {
      return res.status(400).json({ message: "Limite crítico de aceleração deve ser maior que o limite de alerta" });
    }
    
    if (freq_critical <= freq_alert) {
      return res.status(400).json({ message: "Limite crítico de frequência deve ser maior que o limite de alerta" });
    }
    
    // Verificar se empresa e ponte existem
    const company = await Company.findById(company_id);
    if (!company || !company.isActive) {
      return res.status(404).json({ message: "Empresa não encontrada" });
    }
    
    const bridge = await Bridge.findById(bridge_id);
    if (!bridge || !bridge.isActive || bridge.company_id.toString() !== company_id) {
      return res.status(404).json({ message: "Ponte não encontrada ou não pertence à empresa especificada" });
    }
    
    // Verificar se já existe limite para esta ponte
    const existingLimit = await BridgeLimit.findOne({ bridge_id });
    if (existingLimit) {
      return res.status(409).json({ message: "Já existe configuração de limites para esta ponte" });
    }
    
    const limit = new BridgeLimit({
      company_id,
      bridge_id,
      accel_alert,
      accel_critical,
      freq_alert,
      freq_critical
    });
    
    const savedLimit = await limit.save();
    
    // Retornar com dados populados
    const populatedLimit = await BridgeLimit.findById(savedLimit._id)
      .populate('company_id', 'name')
      .populate('bridge_id', 'name')
      .lean();
    
    const limitWithNames = {
      ...populatedLimit,
      company_name: populatedLimit.company_id?.name || 'Empresa não encontrada',
      bridge_name: populatedLimit.bridge_id?.name || 'Ponte não encontrada'
    };
    
    res.status(201).json(limitWithNames);
  } catch (error) {
    console.error("Error creating bridge limit:", error);
    res.status(500).json({ message: "Erro ao criar limite", error: error.message });
  }
});

// PUT /bridge-limits/:id - Atualizar limite
router.put("/:id", async (req, res) => {
  try {
    const { accel_alert, accel_critical, freq_alert, freq_critical } = req.body;
    
    // Validações
    if (typeof accel_alert !== 'number' || accel_alert < 0) {
      return res.status(400).json({ message: "Limite de alerta de aceleração deve ser um número positivo" });
    }
    
    if (typeof accel_critical !== 'number' || accel_critical < 0) {
      return res.status(400).json({ message: "Limite crítico de aceleração deve ser um número positivo" });
    }
    
    if (typeof freq_alert !== 'number' || freq_alert < 0) {
      return res.status(400).json({ message: "Limite de alerta de frequência deve ser um número positivo" });
    }
    
    if (typeof freq_critical !== 'number' || freq_critical < 0) {
      return res.status(400).json({ message: "Limite crítico de frequência deve ser um número positivo" });
    }
    
    if (accel_critical <= accel_alert) {
      return res.status(400).json({ message: "Limite crítico de aceleração deve ser maior que o limite de alerta" });
    }
    
    if (freq_critical <= freq_alert) {
      return res.status(400).json({ message: "Limite crítico de frequência deve ser maior que o limite de alerta" });
    }
    
    const limit = await BridgeLimit.findByIdAndUpdate(
      req.params.id,
      { 
        accel_alert,
        accel_critical,
        freq_alert,
        freq_critical,
        updatedAt: new Date()
      },
      { new: true, runValidators: true }
    ).populate('company_id', 'name').populate('bridge_id', 'name');
    
    if (!limit) {
      return res.status(404).json({ message: "Limite não encontrado" });
    }
    
    const limitWithNames = {
      ...limit.toObject(),
      company_name: limit.company_id?.name || 'Empresa não encontrada',
      bridge_name: limit.bridge_id?.name || 'Ponte não encontrada'
    };
    
    res.json(limitWithNames);
  } catch (error) {
    console.error("Error updating bridge limit:", error);
    res.status(500).json({ message: "Erro ao atualizar limite", error: error.message });
  }
});

// DELETE /bridge-limits/:id - Deletar limite
router.delete("/:id", async (req, res) => {
  try {
    const limit = await BridgeLimit.findByIdAndDelete(req.params.id);
    
    if (!limit) {
      return res.status(404).json({ message: "Limite não encontrado" });
    }
    
    res.json({ message: "Limite removido com sucesso" });
  } catch (error) {
    console.error("Error deleting bridge limit:", error);
    res.status(500).json({ message: "Erro ao remover limite", error: error.message });
  }
});

export default router;

