// src/routes/telemetry.js
import { Router } from "express";
import {
  updateAllBridgeStatuses,
  updateBridgeStatusFor,
  latestByCompany,
  latestByBridge,
} from "../services/telemetry.js";
import BridgeStatus from "../models/bridgeStatus.js"; // opcional: para GET do snapshot

const router = Router();

/* ===========================
 * HEARTBEAT / SNAPSHOT
 * ===========================*/

// Dispara rebuild de snapshot para TODAS as pontes (varre devices agrupados)
router.post("/bridge-status/run", async (_req, res, next) => {
  try {
    const out = await updateAllBridgeStatuses();
    res.json({ ok: true, updated: out.length, bridges: out });
  } catch (e) {
    next(e);
  }
});

// Dispara rebuild do snapshot para UMA ponte específica
router.post("/bridge-status/:companyId/:bridgeId/run", async (req, res, next) => {
  try {
    const { companyId, bridgeId } = req.params;
    const out = await updateBridgeStatusFor(bridgeId, companyId);
    res.json({ ok: true, ...out });
  } catch (e) {
    next(e);
  }
});

// (Opcional) Lê o snapshot salvo (sem recalcular) — útil para telas rápidas
router.get("/bridge-status/:companyId/:bridgeId", async (req, res, next) => {
  try {
    const { companyId, bridgeId } = req.params;
    const doc = await BridgeStatus.findOne({ company_id: companyId, bridge_id: bridgeId }).lean();
    if (!doc) return res.status(404).json({ ok: false, error: "snapshot not found" });
    res.json({ ok: true, snapshot: doc });
  } catch (e) {
    next(e);
  }
});

/* ===========================
 * LATEST (para Dashboard e Bridge Page)
 * ===========================*/

// Últimos valores (accel/freq) + modo_operacao + status active|stale|offline para TODOS os devices da empresa
router.get("/latest/company/:companyId", async (req, res, next) => {
  try {
    const data = await latestByCompany(req.params.companyId);
    res.json(data);
  } catch (e) {
    next(e);
  }
});

// Últimos valores (accel/freq) + modo_operacao + status para TODOS os devices de UMA ponte
router.get("/latest/bridge/:bridgeId", async (req, res, next) => {
  try {
    const data = await latestByBridge(req.params.bridgeId);
    res.json(data);
  } catch (e) {
    next(e);
  }
});

export default router;
