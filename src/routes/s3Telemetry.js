import express from "express";
import {
  accelAggRange, freqAggRange, accelRawRange, freqRawRange,
  accelBoxplot, freqBoxplot,
  accelHist, freqHist,
  telemetrySummary
} from "../controllers/s3Telemetry.js";

const router = express.Router();

// AGG range
router.get("/accel/agg", accelAggRange);
router.get("/freq/agg", freqAggRange);

// RAW range
router.get("/accel/raw", accelRawRange);
router.get("/freq/raw", freqRawRange);

// BOX PLOT
router.get("/accel/boxplot", accelBoxplot);
router.get("/freq/boxplot", freqBoxplot);

// HIST
router.get("/accel/hist", accelHist);
router.get("/freq/hist", freqHist);

// SUMMARY
router.get("/summary", telemetrySummary);

export default router;