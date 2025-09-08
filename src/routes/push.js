
import { Router } from "express";
import { subscribe, vapidKey } from "../controllers/push.js";
const router = Router();
router.get("/vapidPublicKey", vapidKey);
router.post("/subscribe", subscribe);
export default router;
