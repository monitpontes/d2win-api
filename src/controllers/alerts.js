import Alert from "../models/alert.js";
import BridgeLimit from "../models/bridgeLimit.js";

// ---------------- EXISTENTE ----------------
export async function listAlerts(req, res, next) {
  try {
    const q = {};
    if (req.query.bridge_id) q.bridge_id = req.query.bridge_id;
    if (req.query.device_id) q.device_id = req.query.device_id;
    const docs = await Alert.find(q).sort({ ts: -1 }).limit(200);
    res.json(docs);
  } catch (e) { next(e); }
}

// ---------------- NOVO: avaliação por limites do banco ----------------
function levelFrom(value, alert, critical) {
  if (value <= alert) return "normal";
  if (value <= critical) return "warning";
  return "critical";
}

/**
 * POST /alerts/evaluate
 * Body:
 * {
 *   company_id: string(ObjectId),
 *   bridge_id:  string(ObjectId),
 *   device_id:  string,
 *   ts?:        string|Date,
 *   freq_hz?:   number,
 *   accel_ms2?: number
 * }
 *
 * Retorna 201 com os alerts criados (se houver) ou 204 se nenhum alerta foi necessário
 */
export async function evaluateFromMeasurement(req, res, next) {
  try {
    const { company_id, bridge_id, device_id, ts, freq_hz, accel_ms2 } = req.body || {};
    if (!company_id || !bridge_id || !device_id) {
      return res.status(400).json({ message: "company_id, bridge_id e device_id são obrigatórios" });
    }

    const when = ts ? new Date(ts) : new Date();

    // 1) Busca limites da ponte no banco
    const limits = await BridgeLimit.findOne({ company_id, bridge_id }).lean();
    if (!limits) {
      // sem limite => não emite alerta
      return res.status(204).end();
    }

    const created = [];

    // 2) Frequência (se veio no payload)
    if (typeof freq_hz === "number" && Number.isFinite(freq_hz)) {
      const lvl = levelFrom(freq_hz, limits.freq_alert, limits.freq_critical);
      if (lvl !== "normal") {
        const doc = await Alert.create({
          company_id, bridge_id, device_id,
          type: "freq",
          severity: lvl, // "warning" | "critical"
          message:
            lvl === "critical"
              ? `Frequência ${freq_hz.toFixed(2)} Hz > CRÍTICO (${limits.freq_critical} Hz)`
              : `Frequência ${freq_hz.toFixed(2)} Hz > AVISO (${limits.freq_alert} Hz)`,
          ts: when,
          payload: { ts: when, freq_hz },
          status: "open"
        });
        created.push(doc);
      }
    }

    // 3) Aceleração (se veio no payload)
    if (typeof accel_ms2 === "number" && Number.isFinite(accel_ms2)) {
      const lvl = levelFrom(accel_ms2, limits.accel_alert, limits.accel_critical);
      if (lvl !== "normal") {
        const doc = await Alert.create({
          company_id, bridge_id, device_id,
          type: "accel",
          severity: lvl,
          message:
            lvl === "critical"
              ? `Aceleração ${accel_ms2.toFixed(2)} m/s² > CRÍTICO (${limits.accel_critical} m/s²)`
              : `Aceleração ${accel_ms2.toFixed(2)} m/s² > AVISO (${limits.accel_alert} m/s²)`,
          ts: when,
          payload: { ts: when, accel_ms2 },
          status: "open"
        });
        created.push(doc);
      }
    }

    if (created.length === 0) return res.status(204).end();
    return res.status(201).json(created);
  } catch (e) {
    next(e);
  }
}
