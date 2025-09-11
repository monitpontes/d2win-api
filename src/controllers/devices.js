// src/controllers/devices.js
import Device from "../models/device.js";
import DeviceCommand from "../models/deviceCommand.js";

export async function getParams(req, res, next) {
  try {
    const device = await Device.findOne({ device_id: req.params.deviceId });
    if (!device) return res.status(404).json({ error: "Device not found" });

    const p = device.params_current ? device.params_current.toObject() : {};
    res.json({
      device_number: p.device_number ?? device.device_number ?? device._id?.toString().slice(-3),
      intervalo_aquisicao: p.intervalo_aquisicao ?? 1000,        // <<<< VALOR PADRÃO
      amostras: p.amostras ?? 4096,                              // <<<< VALOR PADRÃO
      freq_amostragem: p.freq_amostragem ?? 50,                  // <<<< VALOR PADRÃO
      activity_threshold: p.activity_threshold ?? 0.9,          // <<<< VALOR PADRÃO
      tempo_calibracao: p.tempo_calibracao ?? 5000,             // <<<< VALOR PADRÃO
      modo_operacao: p.modo_operacao ?? device.modo_operacao ?? "aceleracao",
      modo_execucao: p.modo_execucao ?? "teste",             // <<<< VALOR PADRÃO
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

    // Preparar campos para atualizar
    const updateFields = {
      firmware_version: fw,
      params_current: merged,
      last_seen: new Date()
    };

    // Se modo_operacao vier no body, atualizar também
    if (body.modo_operacao) {
      updateFields.modo_operacao = body.modo_operacao;
    }

    // Se infos vier no body, atualizar também  
    if (body.infos) {
      updateFields.infos = body.infos;
    }

    const updated = await Device.findOneAndUpdate(
      { device_id: id },
      { $set: updateFields },
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
