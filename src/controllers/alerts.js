
import Alert from "../models/alert.js";

export async function listAlerts(req, res, next) {
  try {
    const q = {};
    if (req.query.bridge_id) q.bridge_id = req.query.bridge_id;
    if (req.query.device_id) q.device_id = req.query.device_id;
    const docs = await Alert.find(q).sort({ ts: -1 }).limit(200);
    res.json(docs);
  } catch (e) { next(e); }
}
