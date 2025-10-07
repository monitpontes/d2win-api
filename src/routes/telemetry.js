// src/routes/telemetry.js
import { Router } from "express";
import {
  updateAllBridgeStatuses,
  updateBridgeStatusFor,
  latestByCompany,
  latestByBridge,
  historyByBridge,
} from "../services/telemetry.js";
import BridgeStatus from "../models/bridgeStatus.js"; // opcional

const router = Router();

/* ===========================
 * HEARTBEAT / SNAPSHOT
 * ===========================*/

router.post("/bridge-status/run", async (_req, res, next) => {
  try {
    const out = await updateAllBridgeStatuses();
    res.json({ ok: true, updated: out.length, bridges: out });
  } catch (e) {
    next(e);
  }
});

router.post("/bridge-status/:companyId/:bridgeId/run", async (req, res, next) => {
  try {
    const { companyId, bridgeId } = req.params;
    const out = await updateBridgeStatusFor(bridgeId, companyId);
    res.json({ ok: true, ...out });
  } catch (e) {
    next(e);
  }
});

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
 * LATEST
 * ===========================*/

router.get("/latest/company/:companyId", async (req, res, next) => {
  try {
    const data = await latestByCompany(req.params.companyId);
    res.json(data);
  } catch (e) {
    next(e);
  }
});

router.get("/latest/bridge/:bridgeId", async (req, res, next) => {
  try {
    const data = await latestByBridge(req.params.bridgeId);
    res.json(data);
  } catch (e) {
    next(e);
  }
});

/* ===========================
 * HISTORY (para o gráfico)
 * ===========================*/

router.get("/history/bridge/:bridgeId", async (req, res, next) => {
  try {
    const limit = Math.max(1, Math.min(200, Number(req.query.limit) || 10));
    const data = await historyByBridge(req.params.bridgeId, limit);
    res.json(data);
  } catch (e) {
    next(e);
  }
});

export default router;
