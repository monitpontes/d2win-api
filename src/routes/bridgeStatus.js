// src/routes/bridgeStatus.js
import express from "express";
import BridgeStatus from "../models/bridgeStatus.js";

const router = express.Router();

// GET /bridge-status - Listar status de todas as pontes
router.get("/", async (req, res) => {
  try {
    const { company_id } = req.query;
    
    const filter = {};
    if (company_id) filter.company_id = company_id;
    
    const bridgeStatuses = await BridgeStatus.find(filter)
      .sort({ updated_at: -1 })
      .lean();
    
    res.json(bridgeStatuses);
  } catch (error) {
    console.error("Error fetching bridge status:", error);
    res.status(500).json({ message: "Erro ao buscar status das pontes", error: error.message });
  }
});

// GET /bridge-status/:bridgeId - Buscar status de uma ponte específica
router.get("/:bridgeId", async (req, res) => {
  try {
    const bridgeStatus = await BridgeStatus.findOne({ 
      bridge_id: req.params.bridgeId 
    }).lean();
    
    if (!bridgeStatus) {
      return res.status(404).json({ message: "Status da ponte não encontrado" });
    }
    
    res.json(bridgeStatus);
  } catch (error) {
    console.error("Error fetching bridge status:", error);
    res.status(500).json({ message: "Erro ao buscar status da ponte", error: error.message });
  }
});

export default router;

