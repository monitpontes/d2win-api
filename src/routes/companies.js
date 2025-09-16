// src/routes/companies.js
import express from "express";
import Company from "../models/company.js";

const router = express.Router();

// GET /companies - Listar todas as empresas
router.get("/", async (req, res) => {
  try {
    const companies = await Company.find({ isActive: true }).sort({ name: 1 }).lean();
    res.json(companies);
  } catch (error) {
    console.error("Error fetching companies:", error);
    res.status(500).json({ message: "Erro ao buscar empresas", error: error.message });
  }
});

// GET /companies/:id - Buscar empresa por ID
router.get("/:id", async (req, res) => {
  try {
    const company = await Company.findById(req.params.id).lean();
    
    if (!company) {
      return res.status(404).json({ message: "Empresa não encontrada" });
    }
    
    res.json(company);
  } catch (error) {
    console.error("Error fetching company:", error);
    res.status(500).json({ message: "Erro ao buscar empresa", error: error.message });
  }
});

// POST /companies - Criar nova empresa
router.post("/", async (req, res) => {
  try {
    const { name } = req.body;
    
    if (!name || name.trim().length === 0) {
      return res.status(400).json({ message: "Nome da empresa é obrigatório" });
    }
    
    // Verificar se já existe empresa com mesmo nome
    const existingCompany = await Company.findOne({ 
      name: { $regex: new RegExp(`^${name.trim()}$`, 'i') },
      isActive: true 
    });
    
    if (existingCompany) {
      return res.status(409).json({ message: "Já existe uma empresa com este nome" });
    }
    
    const company = new Company({
      name: name.trim(),
      isActive: true
    });
    
    const savedCompany = await company.save();
    res.status(201).json(savedCompany);
  } catch (error) {
    console.error("Error creating company:", error);
    res.status(500).json({ message: "Erro ao criar empresa", error: error.message });
  }
});

// PUT /companies/:id - Atualizar empresa
router.put("/:id", async (req, res) => {
  try {
    const { name } = req.body;
    
    if (!name || name.trim().length === 0) {
      return res.status(400).json({ message: "Nome da empresa é obrigatório" });
    }
    
    // Verificar se já existe outra empresa com mesmo nome
    const existingCompany = await Company.findOne({ 
      _id: { $ne: req.params.id },
      name: { $regex: new RegExp(`^${name.trim()}$`, 'i') },
      isActive: true 
    });
    
    if (existingCompany) {
      return res.status(409).json({ message: "Já existe uma empresa com este nome" });
    }
    
    const company = await Company.findByIdAndUpdate(
      req.params.id,
      { 
        name: name.trim(),
        updatedAt: new Date()
      },
      { new: true, runValidators: true }
    );
    
    if (!company) {
      return res.status(404).json({ message: "Empresa não encontrada" });
    }
    
    res.json(company);
  } catch (error) {
    console.error("Error updating company:", error);
    res.status(500).json({ message: "Erro ao atualizar empresa", error: error.message });
  }
});

// DELETE /companies/:id - Deletar empresa (soft delete)
router.delete("/:id", async (req, res) => {
  try {
    const company = await Company.findByIdAndUpdate(
      req.params.id,
      { 
        isActive: false,
        updatedAt: new Date()
      },
      { new: true }
    );
    
    if (!company) {
      return res.status(404).json({ message: "Empresa não encontrada" });
    }
    
    res.json({ message: "Empresa removida com sucesso" });
  } catch (error) {
    console.error("Error deleting company:", error);
    res.status(500).json({ message: "Erro ao remover empresa", error: error.message });
  }
});

export default router;

