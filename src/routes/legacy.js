
import { Router } from "express";
import { postAcceleration, postFrequency } from "../controllers/legacy.js";
const router = Router();
router.post("/acceleration", postAcceleration);
router.post("/frequency", postFrequency);
export default router;
