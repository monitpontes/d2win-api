// src/routes/ingest.js
import { Router } from "express";
import { ingestAccel } from "../controllers/ingestAccel.js";
import { ingestFrequency } from "../controllers/ingestFreq.js";

const router = Router();

router.post("/accel", ingestAccel);
router.post("/frequency", ingestFrequency);

export default router;


// import { ingestAccel } from "../controllers/ingestAccel.js";
// import { ingestFrequency } from "../controllers/ingestFreq.js";

// router.post("/accel", ingestAccel);
// router.post("/frequency", ingestFrequency);


// import { Router } from "express";
// import { ingestAccel, ingestFrequency } from "../controllers/ingest.js";
// const router = Router();
// router.post("/accel", ingestAccel);
// router.post("/frequency", ingestFrequency);
// export default router;
