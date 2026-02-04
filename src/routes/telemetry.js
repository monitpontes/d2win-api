// src/routes/telemetry.js
import { Router } from "express";
import {
  updateAllBridgeStatuses,
  updateBridgeStatusFor,
  latestByCompany,
  latestByBridge,
  historyByBridge,
} from "../services/telemetry.js";
import BridgeStatus from "../models/bridgeStatus.js"; // opcional

const router = Router();

/* ===========================
 * HEARTBEAT / SNAPSHOT
 * ===========================*/

router.post("/bridge-status/run", async (_req, res, next) => {
  try {
    const out = await updateAllBridgeStatuses();
    res.json({ ok: true, updated: out.length, bridges: out });
  } catch (e) {
    next(e);
  }
});

router.post("/bridge-status/:companyId/:bridgeId/run", async (req, res, next) => {
  try {
    const { companyId, bridgeId } = req.params;
    const out = await updateBridgeStatusFor(bridgeId, companyId);
    res.json({ ok: true, ...out });
  } catch (e) {
    next(e);
  }
});

router.get("/bridge-status/:companyId/:bridgeId", async (req, res, next) => {
  try {
    const { companyId, bridgeId } = req.params;
    const doc = await BridgeStatus.findOne({ company_id: companyId, bridge_id: bridgeId }).lean();
    if (!doc) return res.status(404).json({ ok: false, error: "snapshot not found" });
    res.json({ ok: true, snapshot: doc });
  } catch (e) {
    next(e);
  }
});

/* ===========================
 * LATEST (último dado por ponte/empresa)
 * ===========================*/

router.get("/latest/company/:companyId", async (req, res, next) => {
  try {
    const data = await latestByCompany(req.params.companyId);
    res.json(data);
  } catch (e) {
    next(e);
  }
});

router.get("/latest/bridge/:bridgeId", async (req, res, next) => {
  try {
    const data = await latestByBridge(req.params.bridgeId);
    res.json(data);
  } catch (e) {
    next(e);
  }
});

/* ===========================
 * HISTORY (para gráficos)
 * ===========================*/

router.get("/history/bridge/:bridgeId", async (req, res, next) => {
  try {
    // 🔒 limite fixo de segurança: sempre retorna no máximo 10 registros
    const limit = 10; // mesmo se o front mandar outro valor
    const data = await historyByBridge(req.params.bridgeId, limit);

    if (!data || !data.items) return res.status(404).json({ ok: false, error: "sem dados recentes" });

    res.json({
      ok: true,
      count: data.items.length,
      limit,
      ...data,
    });
  } catch (e) {
    next(e);
  }
});

// =====================================================
// ✅ TESTE WEBSOCKET (provisório)
// Use apenas para validar se o WS está emitindo eventos.
// TODO: remover quando o frontend estiver integrado.
// =====================================================

// GET /telemetry/websocket/accel/:bridgeId
router.get("/websocket/accel/:bridgeId", (req, res) => {
  const io = globalThis.__io;
  const { bridgeId } = req.params;

  if (!io) {
    return res.status(500).json({
      ok: false,
      error: "Socket.IO não inicializado (io=null). Verifique se app.js criou o io.",
    });
  }

  io.to(`bridge:${bridgeId}`).emit("telemetry", {
    type: "accel",
    bridge_id: bridgeId,
    ts: new Date().toISOString(),
    payload: {
      axis: "z",
      value: 9.81,
      rms: 9.70,
      ax: null,
      ay: null,
      az: 9.81,
      severity: "normal",
      _test: true,
    },
  });

  return res.json({ ok: true, emitted: "telemetry", room: `bridge:${bridgeId}`, type: "accel" });
});

// GET /telemetry/websocket/freq/:bridgeId
router.get("/websocket/freq/:bridgeId", (req, res) => {
  const io = globalThis.__io;
  const { bridgeId } = req.params;

  if (!io) {
    return res.status(500).json({
      ok: false,
      error: "Socket.IO não inicializado (io=null). Verifique se app.js criou o io.",
    });
  }

  io.to(`bridge:${bridgeId}`).emit("telemetry", {
    type: "freq",
    bridge_id: bridgeId,
    ts: new Date().toISOString(),
    payload: {
      status: "atividade_detectada",
      fs: 50,
      n: 4096,
      peaks: [
        { f: 3.5, mag: 1000 },
        { f: 3.4, mag: 800 },
      ],
      severity: "normal",
      _test: true,
    },
  });

  return res.json({ ok: true, emitted: "telemetry", room: `bridge:${bridgeId}`, type: "freq" });
});

//// TODO: remover esta rota /ws-test quando o frontend estiver integrado (é só para validação manual)
// ---- ROTA DE TESTE DE WEBSOCKET (em app.js) ----
router.get("/ws-test", (req, res) => {
  const io = globalThis.__io;
  const bridge_id = req.query.bridge_id;

  if (!io) return res.status(500).json({ ok: false, error: "Socket.IO not initialized" });

  const msg = {
    type: "test",
    bridge_id: bridge_id ?? null,
    ts: new Date().toISOString(),
    payload: { hello: "ws ok" }
  };

  if (bridge_id) io.to(`bridge:${bridge_id}`).emit("telemetry", msg);
  else io.emit("telemetry", msg);

  return res.json({ ok: true, room: bridge_id ? `bridge:${bridge_id}` : "broadcast" });
});



export default router;
