
import PushSub from "../models/pushSub.js";
import { getVapidPublicKey } from "../services/notify.js";

export function vapidKey(req, res) {
  res.json({ publicKey: getVapidPublicKey() });
}

// body: { endpoint, keys:{p256dh,auth}, recipient_id?, bridge_id?, company_id? }
export async function subscribe(req, res, next) {
  try {
    const { endpoint, keys, recipient_id, bridge_id, company_id } = req.body || {};
    if (!endpoint || !keys) return res.status(400).json({ error: "Missing endpoint/keys" });
    const doc = await PushSub.findOneAndUpdate(
      { endpoint },
      { $set: { keys, recipient_id, bridge_id, company_id } },
      { upsert: true, new: true }
    );
    res.status(201).json({ ok: true, id: doc._id });
  } catch (e) { next(e); }
}
