// src/routes/health.js
import { Router } from "express";
import { dbHealthSnapshot } from "../lib/db.js";

const router = Router();

router.get("/health", async (_req, res) => {
  const snap = await dbHealthSnapshot();
  const http = snap.ok ? 200 : 500;
  res.status(http).json({
    ok: snap.ok,
    state: snap.state,          // connected | connecting | disconnected
    readyState: snap.readyState, // 0=disconnected,1=connected,2=connecting,3=disconnecting
    lastConnectedAt: snap.lastConnectedAt,
    lastError: snap.lastError,
    uptime: process.uptime(),
    ts: new Date().toISOString(),
  });
});

// opcional: endpoints estilo Kubernetes
router.get("/readyz", async (_req, res) => {
  const snap = await dbHealthSnapshot();
  res.status(snap.ok ? 200 : 503).send(snap.ok ? "ok" : "not-ready");
});

router.get("/livez", (_req, res) => {
  res.status(200).send("alive");
});

export default router;
