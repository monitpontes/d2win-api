// src/routes/s3Telemetry.js
import express from "express";
import {
  accelAggRange,
  freqAggRange,
  accelRawRange,
  freqRawRange,
  accelSchema,
  freqSchema,
  accelRawExtrema,
  freqRawExtrema,
} from "../controllers/s3Telemetry.js";

const router = express.Router();

// AGG range
router.get("/accel/agg", accelAggRange);
router.get("/freq/agg", freqAggRange);

// RAW range
router.get("/accel/raw", accelRawRange);
router.get("/freq/raw", freqRawRange);

// SCHEMA (RAW/AGG)
router.get("/accel/schema", accelSchema);
router.get("/freq/schema", freqSchema);

// Extrema (RAW)
router.get("/accel/raw/extrema", accelRawExtrema);
router.get("/freq/raw/extrema", freqRawExtrema);

export default router;