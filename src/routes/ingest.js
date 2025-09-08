
import { Router } from "express";
import { ingestAccel, ingestFrequency } from "../controllers/ingest.js";
const router = Router();
router.post("/accel", ingestAccel);
router.post("/frequency", ingestFrequency);
export default router;
