// src/routes/bridges.js
import express from "express";
import Bridge from "../models/bridge.js";
import Company from "../models/company.js";

const router = express.Router();

// GET /bridges - Listar todas as pontes
router.get("/", async (req, res) => {
  try {
    const { company_id } = req.query;
    
    const filter = { isActive: true };
    if (company_id) {
      filter.company_id = company_id;
    }
    
    const bridges = await Bridge.find(filter)
      .populate('company_id', 'name')
      .sort({ name: 1 })
      .lean();
    
    // Adicionar company_name para facilitar uso no frontend
    const bridgesWithNames = bridges.map(bridge => ({
      ...bridge,
      company_name: bridge.company_id?.name || 'Empresa não encontrada'
    }));
    
    res.json(bridgesWithNames);
  } catch (error) {
    console.error("Error fetching bridges:", error);
    res.status(500).json({ message: "Erro ao buscar pontes", error: error.message });
  }
});

// GET /bridges/:id - Buscar ponte por ID
router.get("/:id", async (req, res) => {
  try {
    const bridge = await Bridge.findById(req.params.id)
      .populate('company_id', 'name')
      .lean();
    
    if (!bridge) {
      return res.status(404).json({ message: "Ponte não encontrada" });
    }
    
    // Adicionar company_name
    const bridgeWithName = {
      ...bridge,
      company_name: bridge.company_id?.name || 'Empresa não encontrada'
    };
    
    res.json(bridgeWithName);
  } catch (error) {
    console.error("Error fetching bridge:", error);
    res.status(500).json({ message: "Erro ao buscar ponte", error: error.message });
  }
});

// POST /bridges - Criar nova ponte
router.post("/", async (req, res) => {
  try {
    const { name, company_id, location, description } = req.body;
    
    if (!name || name.trim().length === 0) {
      return res.status(400).json({ message: "Nome da ponte é obrigatório" });
    }
    
    if (!company_id) {
      return res.status(400).json({ message: "ID da empresa é obrigatório" });
    }
    
    // Verificar se a empresa existe
    const company = await Company.findById(company_id);
    if (!company || !company.isActive) {
      return res.status(404).json({ message: "Empresa não encontrada" });
    }
    
    // Verificar se já existe ponte com mesmo nome na mesma empresa
    const existingBridge = await Bridge.findOne({ 
      name: { $regex: new RegExp(`^${name.trim()}$`, 'i') },
      company_id,
      isActive: true 
    });
    
    if (existingBridge) {
      return res.status(409).json({ message: "Já existe uma ponte com este nome nesta empresa" });
    }
    
    const bridge = new Bridge({
      name: name.trim(),
      company_id,
      location: location?.trim() || '',
      description: description?.trim() || '',
      isActive: true
    });
    
    const savedBridge = await bridge.save();
    
    // Retornar com dados da empresa populados
    const populatedBridge = await Bridge.findById(savedBridge._id)
      .populate('company_id', 'name')
      .lean();
    
    const bridgeWithName = {
      ...populatedBridge,
      company_name: populatedBridge.company_id?.name || 'Empresa não encontrada'
    };
    
    res.status(201).json(bridgeWithName);
  } catch (error) {
    console.error("Error creating bridge:", error);
    res.status(500).json({ message: "Erro ao criar ponte", error: error.message });
  }
});

// PUT /bridges/:id - Atualizar ponte
router.put("/:id", async (req, res) => {
  try {
    const { name, company_id, location, description } = req.body;
    
    if (!name || name.trim().length === 0) {
      return res.status(400).json({ message: "Nome da ponte é obrigatório" });
    }
    
    if (!company_id) {
      return res.status(400).json({ message: "ID da empresa é obrigatório" });
    }
    
    // Verificar se a empresa existe
    const company = await Company.findById(company_id);
    if (!company || !company.isActive) {
      return res.status(404).json({ message: "Empresa não encontrada" });
    }
    
    // Verificar se já existe outra ponte com mesmo nome na mesma empresa
    const existingBridge = await Bridge.findOne({ 
      _id: { $ne: req.params.id },
      name: { $regex: new RegExp(`^${name.trim()}$`, 'i') },
      company_id,
      isActive: true 
    });
    
    if (existingBridge) {
      return res.status(409).json({ message: "Já existe uma ponte com este nome nesta empresa" });
    }
    
    const bridge = await Bridge.findByIdAndUpdate(
      req.params.id,
      { 
        name: name.trim(),
        company_id,
        location: location?.trim() || '',
        description: description?.trim() || '',
        updatedAt: new Date()
      },
      { new: true, runValidators: true }
    ).populate('company_id', 'name');
    
    if (!bridge) {
      return res.status(404).json({ message: "Ponte não encontrada" });
    }
    
    const bridgeWithName = {
      ...bridge.toObject(),
      company_name: bridge.company_id?.name || 'Empresa não encontrada'
    };
    
    res.json(bridgeWithName);
  } catch (error) {
    console.error("Error updating bridge:", error);
    res.status(500).json({ message: "Erro ao atualizar ponte", error: error.message });
  }
});

// DELETE /bridges/:id - Deletar ponte (soft delete)
router.delete("/:id", async (req, res) => {
  try {
    const bridge = await Bridge.findByIdAndUpdate(
      req.params.id,
      { 
        isActive: false,
        updatedAt: new Date()
      },
      { new: true }
    );
    
    if (!bridge) {
      return res.status(404).json({ message: "Ponte não encontrada" });
    }
    
    res.json({ message: "Ponte removida com sucesso" });
  } catch (error) {
    console.error("Error deleting bridge:", error);
    res.status(500).json({ message: "Erro ao remover ponte", error: error.message });
  }
});

export default router;

