// src/routes/devicesCrud.js
import express from "express";
import Device from "../models/device.js";
import Company from "../models/company.js";
import Bridge from "../models/bridge.js";

const router = express.Router();

// GET /devices-crud - Listar todos os dispositivos
router.get("/", async (req, res) => {
  try {
    const { company_id, bridge_id } = req.query;
    
    const filter = { isActive: true };
    if (company_id) filter.company_id = company_id;
    if (bridge_id) filter.bridge_id = bridge_id;
    
    const devices = await Device.find(filter)
      .populate('company_id', 'name')
      .populate('bridge_id', 'name')
      .sort({ device_id: 1 })
      .lean();
    
    // Adicionar nomes para facilitar uso no frontend
    const devicesWithNames = devices.map(device => ({
      ...device,
      company_name: device.company_id?.name || 'Empresa não encontrada',
      bridge_name: device.bridge_id?.name || 'Ponte não encontrada'
    }));
    
    res.json(devicesWithNames);
  } catch (error) {
    console.error("Error fetching devices:", error);
    res.status(500).json({ message: "Erro ao buscar dispositivos", error: error.message });
  }
});

// GET /devices-crud/:id - Buscar dispositivo por ID
router.get("/:id", async (req, res) => {
  try {
    const device = await Device.findById(req.params.id)
      .populate('company_id', 'name')
      .populate('bridge_id', 'name')
      .lean();
    
    if (!device) {
      return res.status(404).json({ message: "Dispositivo não encontrado" });
    }
    
    const deviceWithNames = {
      ...device,
      company_name: device.company_id?.name || 'Empresa não encontrada',
      bridge_name: device.bridge_id?.name || 'Ponte não encontrada'
    };
    
    res.json(deviceWithNames);
  } catch (error) {
    console.error("Error fetching device:", error);
    res.status(500).json({ message: "Erro ao buscar dispositivo", error: error.message });
  }
});

// POST /devices-crud - Criar novo dispositivo
router.post("/", async (req, res) => {
  try {
    const { device_id, company_id, bridge_id } = req.body;
    
    if (!device_id || device_id.trim().length === 0) {
      return res.status(400).json({ message: "ID do dispositivo é obrigatório" });
    }
    
    if (!company_id) {
      return res.status(400).json({ message: "ID da empresa é obrigatório" });
    }
    
    if (!bridge_id) {
      return res.status(400).json({ message: "ID da ponte é obrigatório" });
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
    
    // Verificar se já existe dispositivo com mesmo ID
    const existingDevice = await Device.findOne({ 
      device_id: device_id.trim(),
      isActive: true 
    });
    
    if (existingDevice) {
      return res.status(409).json({ message: "Já existe um dispositivo com este ID" });
    }
    
    const device = new Device({
      device_id: device_id.trim(),
      company_id,
      bridge_id,
      isActive: true
    });
    
    const savedDevice = await device.save();
    
    // Retornar com dados populados
    const populatedDevice = await Device.findById(savedDevice._id)
      .populate('company_id', 'name')
      .populate('bridge_id', 'name')
      .lean();
    
    const deviceWithNames = {
      ...populatedDevice,
      company_name: populatedDevice.company_id?.name || 'Empresa não encontrada',
      bridge_name: populatedDevice.bridge_id?.name || 'Ponte não encontrada'
    };
    
    res.status(201).json(deviceWithNames);
  } catch (error) {
    console.error("Error creating device:", error);
    res.status(500).json({ message: "Erro ao criar dispositivo", error: error.message });
  }
});

// PUT /devices-crud/:id - Atualizar dispositivo
router.put("/:id", async (req, res) => {
  try {
    const { device_id, company_id, bridge_id } = req.body;
    
    if (!device_id || device_id.trim().length === 0) {
      return res.status(400).json({ message: "ID do dispositivo é obrigatório" });
    }
    
    if (!company_id) {
      return res.status(400).json({ message: "ID da empresa é obrigatório" });
    }
    
    if (!bridge_id) {
      return res.status(400).json({ message: "ID da ponte é obrigatório" });
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
    
    // Verificar se já existe outro dispositivo com mesmo ID
    const existingDevice = await Device.findOne({ 
      _id: { $ne: req.params.id },
      device_id: device_id.trim(),
      isActive: true 
    });
    
    if (existingDevice) {
      return res.status(409).json({ message: "Já existe um dispositivo com este ID" });
    }
    
    const device = await Device.findByIdAndUpdate(
      req.params.id,
      { 
        device_id: device_id.trim(),
        company_id,
        bridge_id,
        updatedAt: new Date()
      },
      { new: true, runValidators: true }
    ).populate('company_id', 'name').populate('bridge_id', 'name');
    
    if (!device) {
      return res.status(404).json({ message: "Dispositivo não encontrado" });
    }
    
    const deviceWithNames = {
      ...device.toObject(),
      company_name: device.company_id?.name || 'Empresa não encontrada',
      bridge_name: device.bridge_id?.name || 'Ponte não encontrada'
    };
    
    res.json(deviceWithNames);
  } catch (error) {
    console.error("Error updating device:", error);
    res.status(500).json({ message: "Erro ao atualizar dispositivo", error: error.message });
  }
});

// DELETE /devices-crud/:id - Deletar dispositivo (soft delete)
router.delete("/:id", async (req, res) => {
  try {
    const device = await Device.findByIdAndUpdate(
      req.params.id,
      { 
        isActive: false,
        updatedAt: new Date()
      },
      { new: true }
    );
    
    if (!device) {
      return res.status(404).json({ message: "Dispositivo não encontrado" });
    }
    
    res.json({ message: "Dispositivo removido com sucesso" });
  } catch (error) {
    console.error("Error deleting device:", error);
    res.status(500).json({ message: "Erro ao remover dispositivo", error: error.message });
  }
});

export default router;

