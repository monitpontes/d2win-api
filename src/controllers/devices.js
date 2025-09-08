
import Device from "../models/device.js";
import DeviceCommand from "../models/deviceCommand.js";

export async function getParams(req, res, next) {
  try {
    const device = await Device.findOne({ device_id: req.params.deviceId });
    if (!device) return res.status(404).json({ error: "Device not found" });

    const p = device.params_current ? device.params_current.toObject() : {};
    res.json({
      device_number: p.device_number ?? device.device_number ?? device._id?.toString().slice(-3),
      intervalo_aquisicao: p.intervalo_aquisicao,
      amostras: p.amostras,
      freq_amostragem: p.freq_amostragem,
      activity_threshold: p.activity_threshold,
      modo_operacao: p.modo_operacao,
      modo_execucao: p.modo_execucao,
      modo_teste: p.modo_teste ?? "completo"
    });
  } catch (e) { next(e); }
}

export async function patchParams(req, res, next) {
  try {
    const id = req.params.deviceId;
    const body = req.body || {};
    const fw = body.firmware_version;
    const incoming = body.params || {};

    const device = await Device.findOne({ device_id: id });
    const current = device?.params_current ? device.params_current.toObject() : {};
    const merged = { ...current, ...incoming, modo_teste: "completo" };

    const updated = await Device.findOneAndUpdate(
      { device_id: id },
      { $set: { firmware_version: fw, params_current: merged, last_seen: new Date() } },
      { new: true, upsert: true, setDefaultsOnInsert: true }
    );
    res.status(200).json({ ok: true, device_id: updated.device_id });
  } catch (e) { next(e); }
}

export async function getRestartFlag(req, res, next) {
  try {
    const pending = await DeviceCommand.findOne({ device_id: req.params.deviceId, type: "restart", status: "pending" })
      .sort({ issued_at: -1 });
    if (!pending) return res.status(200).send("0");
    pending.status = "sent";
    await pending.save();
    return res.status(200).send("1");
  } catch (e) { next(e); }
}
