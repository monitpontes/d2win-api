// src/controllers/devices.js
import Device from "../models/device.js";
import DeviceCommand from "../models/deviceCommand.js";

export async function getParams(req, res, next) {
  try {
    const device = await Device.findOne({ device_id: req.params.deviceId });
    if (!device) return res.status(404).json({ error: "Device not found" });

    // Tratamento correto para Map do MongoDB
    const p = {};
    if (device.params_current && device.params_current instanceof Map) {
      // Se é um Map, converte para objeto
      for (let [key, value] of device.params_current) {
        p[key] = value;
      }
    } else if (device.params_current) {
      // Se já é um objeto, usa diretamente
      Object.assign(p, device.params_current);
    }

    console.log("=== DEBUG PARAMS ===");
    console.log("device.params_current:", device.params_current);
    console.log("p object:", p);
    console.log("activity_threshold:", p.activity_threshold);
    console.log("modo_execucao:", p.modo_execucao);
    console.log("==================");

    res.json({
      device_number: p.device_number ?? device.device_number ?? device._id?.toString().slice(-3),
      intervalo_aquisicao: p.intervalo_aquisicao ?? 1000,
      amostras: p.amostras ?? 4096,
      freq_amostragem: p.freq_amostragem ?? 50,
      activity_threshold: p.activity_threshold ?? 0.9,
      tempo_calibracao: p.tempo_calibracao ?? 5000,
      modo_operacao: p.modo_operacao ?? device.modo_operacao ?? "aceleracao",
      modo_execucao: p.modo_execucao ?? "debug",
      modo_teste: p.modo_teste ?? "completo"
    });
  } catch (e) { 
    console.error("Erro em getParams:", e);
    next(e); 
  }
}

export async function patchParams(req, res, next) {
  try {
    const id = req.params.deviceId;
    const body = req.body || {};
    const fw = body.firmware_version;
    const incoming = body.params || {};

    const device = await Device.findOne({ device_id: id });
    
    // Tratamento correto para Map do MongoDB
    const current = {};
    if (device?.params_current && device.params_current instanceof Map) {
      for (let [key, value] of device.params_current) {
        current[key] = value;
      }
    } else if (device?.params_current) {
      Object.assign(current, device.params_current);
    }

    const merged = { ...current, ...incoming, modo_teste: "completo" };

    // Preparar campos para atualizar
    const updateFields = {
      params_current: merged,
      last_seen: new Date()
    };

    // Só atualiza firmware_version se vier no body
    if (fw) {
      updateFields.firmware_version = fw;
    }

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
  } catch (e) { 
    console.error("Erro em patchParams:", e);
    next(e); 
  }
}

export async function getRestartFlag(req, res, next) {
  try {
    const pending = await DeviceCommand.findOne({ 
      device_id: req.params.deviceId, 
      type: "restart", 
      status: "pending" 
    }).sort({ issued_at: -1 });
    
    if (!pending) return res.status(200).send("0");
    
    pending.status = "sent";
    await pending.save();
    
    return res.status(200).send("1");
  } catch (e) { 
    console.error("Erro em getRestartFlag:", e);
    next(e); 
  }
}

