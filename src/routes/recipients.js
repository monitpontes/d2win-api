
import { Router } from "express";
import { list, create, update, remove } from "../controllers/recipients.js";
const router = Router();
router.get("/", list);
router.post("/", create);
router.patch("/:id", update);
router.delete("/:id", remove);
export default router;
