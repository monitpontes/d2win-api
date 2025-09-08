
import { Router } from "express";
import { getParams, patchParams, getRestartFlag } from "../controllers/devices.js";
const router = Router();
router.get("/:deviceId/params", getParams);
router.patch("/:deviceId/params", patchParams);
router.get("/restart/:deviceId", getRestartFlag);
export default router;
