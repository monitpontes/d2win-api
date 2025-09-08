
import Recipient from "../models/recipient.js";

export async function list(req, res, next) {
  try {
    const q = {};
    if (req.query.bridge_id) q.bridge_id = req.query.bridge_id;
    if (req.query.company_id) q.company_id = req.query.company_id;
    const docs = await Recipient.find(q).sort({ createdAt: -1 });
    res.json(docs);
  } catch (e) { next(e); }
}

export async function create(req, res, next) {
  try {
    const r = await Recipient.create(req.body);
    res.status(201).json(r);
  } catch (e) { next(e); }
}

export async function update(req, res, next) {
  try {
    const r = await Recipient.findByIdAndUpdate(req.params.id, { $set: req.body }, { new: true });
    res.json(r);
  } catch (e) { next(e); }
}

export async function remove(req, res, next) {
  try {
    await Recipient.findByIdAndDelete(req.params.id);
    res.status(204).end();
  } catch (e) { next(e); }
}
