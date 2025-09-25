import { Router } from "express";
import { listAlerts, evaluateFromMeasurement } from "../controllers/alerts.js";

const router = Router();

router.get("/", listAlerts);

// NOVO: usa os limites do Mongo para decidir alertas
router.post("/evaluate", evaluateFromMeasurement);

export default router;
