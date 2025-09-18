// api/heartbeat.js
import { runBridgeHeartbeatOnce } from "../src/services/bridgeHeartbeat.js";
import { connectMongo } from "../src/lib/db.js";

// cache da conexão para evitar reconectar a cada invocação
let bootPromise;

async function boot() {
  if (!bootPromise) {
    const uri = process.env.MONGO_URI;
    if (!uri) throw new Error("Missing MONGO_URI");
    bootPromise = connectMongo(uri);
  }
  return bootPromise;
}

export default async function handler(req, res) {
  try {
    if (req.method !== "GET") {
      res.status(405).json({ ok: false, message: "Use GET" });
      return;
    }

    // protege com token se definido
    const REQUIRED = process.env.HEARTBEAT_TOKEN || "";
    if (REQUIRED) {
      const auth = (req.headers.authorization || "").replace(/^Bearer\s+/i, "");
      const provided = auth || req.query.token;
      if (provided !== REQUIRED) {
        res.status(401).json({ ok: false, message: "unauthorized" });
        return;
      }
    }

    await boot(); // garante uma única conexão compartilhada
    const r = await runBridgeHeartbeatOnce();

    res.status(200).json({
      ok: true,
      bridges_processed: r.processed,
      interval_ms: r.interval_ms,
      active_ms: r.active_ms,
      stale_ms: r.stale_ms,
    });
  } catch (error) {
    console.error("heartbeat error:", error);
    res.status(500).json({ ok: false, message: error.message });
  }
}
