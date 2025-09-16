// src/routes/telemetry.js
import { Router } from "express";
import { updateAllBridgeStatuses, updateBridgeStatusFor } from "../services/telemetry.js";

const router = Router();

// dispara para todas as pontes
router.post("/bridge-status/run", async (req, res, next) => {
  try {
    const out = await updateAllBridgeStatuses();
    res.json({ ok: true, updated: out.length, bridges: out });
  } catch (e) { next(e); }
});

// dispara para uma ponte específica
router.post("/bridge-status/:companyId/:bridgeId/run", async (req, res, next) => {
  try {
    const { companyId, bridgeId } = req.params;
    const out = await updateBridgeStatusFor(bridgeId, companyId);
    res.json({ ok: true, ...out });
  } catch (e) { next(e); }
});

export default router;