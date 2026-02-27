import express from "express";
import { accelAggregate, freqAggregate } from "../controllers/telemetryAggregate.js";

const router = express.Router();

// /api/telemetry/accel/aggregate?granularity=hourly&year=2025&month=11
router.get("/accel/aggregate", accelAggregate);

// /api/telemetry/freq/aggregate?granularity=hourly&year=2025&month=11
router.get("/freq/aggregate", freqAggregate);

export default router;