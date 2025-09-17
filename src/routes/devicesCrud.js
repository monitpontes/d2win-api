// src/routes/devicesCrud.js
import express from "express";
import Device from "../models/device.js";
import Company from "../models/company.js";
import Bridge from "../models/bridge.js";

const router = express.Router();

/* ----------------------------- helpers ----------------------------- */
const NUM_FIELDS_PARAMS = new Set([
  "tempo_calibracao",
  "intervalo_aquisicao",
  "amostras",
  "freq_amostragem",
  "activity_threshold",
]);

const BOOL_FIELDS_INFOS = new Set(["sDOK", "restartPending"]);
const NUM_FIELDS_INFOS = new Set(["dados_pendentes"]);

function toNumMaybe(v) {
  if (v === "" || v === null || v === undefined) return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}
function toBoolMaybe(v) {
  if (v === "" || v === null || v === undefined) return undefined;
  if (typeof v === "boolean") return v;
  if (v === "true" || v === "1" || v === 1) return true;
  if (v === "false" || v === "0" || v === 0) return false;
  return undefined;
}

function buildSetFromBody(body = {}) {
  // Monta um $set só com campos válidos/nomeados da API
  const $set = {};

  // meta.*
  if (body.meta && typeof body.meta === "object") {
    if (typeof body.meta.location === "string") $set["meta.location"] = body.meta.location.trim();
    if (typeof body.meta.axis === "string") $set["meta.axis"] = body.meta.axis.trim();
  }

  // params_current.*
  const pc = body.params_current || {};
  const paramsKeys = [
    "modo_teste",
    "tempo_calibracao",
    "intervalo_aquisicao",
    "amostras",
    "freq_amostragem",
    "activity_threshold",
    "modo_execucao",
  ];
  for (const k of paramsKeys) {
    if (pc[k] !== undefined) {
      if (NUM_FIELDS_PARAMS.has(k)) {
        const n = toNumMaybe(pc[k]);
        if (n !== undefined) $set[`params_current.${k}`] = n;
      } else if (typeof pc[k] === "string") {
        $set[`params_current.${k}`] = pc[k].trim();
      }
    }
  }

  // infos.*
  const infos = body.infos || {};
  const infoKeys = [
    "dados_pendentes",
    "sDOK",
    "restartPending",
    "firmware_version",
    "last_seen",
    "modo_operacao",
  ];
  for (const k of infoKeys) {
    if (infos[k] !== undefined) {
      if (BOOL_FIELDS_INFOS.has(k)) {
        const b = toBoolMaybe(infos[k]);
        if (b !== undefined) $set[`infos.${k}`] = b;
      } else if (NUM_FIELDS_INFOS.has(k)) {
        const n = toNumMaybe(infos[k]);
        if (n !== undefined) $set[`infos.${k}`] = n;
      } else if (typeof infos[k] === "string") {
        $set[`infos.${k}`] = infos[k].trim();
      }
    }
  }

  // last_seen também pode vir na raiz
  if (body.last_seen) $set["infos.last_seen"] = String(body.last_seen);

  return $set;
}

function defaultParamsCurrent() {
  return {
    modo_teste: "completo",
    tempo_calibracao: 5000,
    intervalo_aquisicao: 1000,
    amostras: 4096,
    freq_amostragem: 2000,
    activity_threshold: 1,
    modo_execucao: "debug", // "debug" | "operacao" | "teste"
  };
}
function defaultInfos() {
  return {
    dados_pendentes: 0,
    sDOK: true,
    restartPending: false,
    firmware_version: "3.4.0_autoregister",
    last_seen: null,
    modo_operacao: "aceleracao", // "aceleracao" | "frequencia"
  };
}

/* ------------------------------- GETs ------------------------------- */

// GET /devices-crud
router.get("/", async (req, res) => {
  try {
    const { company_id, bridge_id } = req.query;

    const filter = { isActive: true };
    if (company_id) filter.company_id = company_id;
    if (bridge_id) filter.bridge_id = bridge_id;

    const devices = await Device.find(filter)
      .populate("company_id", "name")
      .populate("bridge_id", "name")
      .sort({ device_id: 1 })
      .lean();

    const devicesWithNames = devices.map((d) => ({
      ...d,
      company_name: d.company_id?.name || "Empresa não encontrada",
      bridge_name: d.bridge_id?.name || "Ponte não encontrada",
    }));

    res.json(devicesWithNames);
  } catch (error) {
    console.error("Error fetching devices:", error);
    res.status(500).json({ message: "Erro ao buscar dispositivos", error: error.message });
  }
});

// GET /devices-crud/:id
router.get("/:id", async (req, res) => {
  try {
    const device = await Device.findById(req.params.id)
      .populate("company_id", "name")
      .populate("bridge_id", "name")
      .lean();

    if (!device) return res.status(404).json({ message: "Dispositivo não encontrado" });

    res.json({
      ...device,
      company_name: device.company_id?.name || "Empresa não encontrada",
      bridge_name: device.bridge_id?.name || "Ponte não encontrada",
    });
  } catch (error) {
    console.error("Error fetching device:", error);
    res.status(500).json({ message: "Erro ao buscar dispositivo", error: error.message });
  }
});

/* ------------------------------- POST ------------------------------- */

// POST /devices-crud  (criação manual; ponte/empresa obrigatórias)
router.post("/", async (req, res) => {
  try {
    const { device_id, company_id, bridge_id } = req.body;

    if (!device_id || !String(device_id).trim()) {
      return res.status(400).json({ message: "ID do dispositivo é obrigatório" });
    }
    if (!company_id) return res.status(400).json({ message: "ID da empresa é obrigatório" });
    if (!bridge_id) return res.status(400).json({ message: "ID da ponte é obrigatório" });

    const company = await Company.findById(company_id);
    if (!company || !company.isActive) {
      return res.status(404).json({ message: "Empresa não encontrada" });
    }

    const bridge = await Bridge.findById(bridge_id);
    if (!bridge || !bridge.isActive || bridge.company_id.toString() !== company_id) {
      return res
        .status(404)
        .json({ message: "Ponte não encontrada ou não pertence à empresa especificada" });
    }

    const exists = await Device.findOne({ device_id: String(device_id).trim(), isActive: true });
    if (exists) return res.status(409).json({ message: "Já existe um dispositivo com este ID" });

    // defaults + override pelo body
    const doc = new Device({
      device_id: String(device_id).trim(),
      company_id,
      bridge_id,
      isActive: true,
      meta: {
        location: req.body?.meta?.location || bridge.location || "",
        axis: req.body?.meta?.axis || "z",
      },
      params_current: { ...defaultParamsCurrent(), ...(req.body?.params_current || {}) },
      infos: { ...defaultInfos(), ...(req.body?.infos || {}) },
    });

    const saved = await doc.save();
    const populated = await Device.findById(saved._id)
      .populate("company_id", "name")
      .populate("bridge_id", "name")
      .lean();

    res.status(201).json({
      ...populated,
      company_name: populated.company_id?.name || "Empresa não encontrada",
      bridge_name: populated.bridge_id?.name || "Ponte não encontrada",
    });
  } catch (error) {
    console.error("Error creating device:", error);
    res.status(500).json({ message: "Erro ao criar dispositivo", error: error.message });
  }
});

/* ------------------------------- PUT ------------------------------- */

// PUT /devices-crud/:id
// Agora aceita atualização parcial. Só valida device_id/empresa/ponte SE você mandar.
router.put("/:id", async (req, res) => {
  try {
    const updates = {};
    const { device_id, company_id, bridge_id } = req.body;

    // identifiers opcionais
    if (device_id !== undefined) {
      const trimmed = String(device_id || "").trim();
      if (!trimmed) {
        return res.status(400).json({ message: "ID do dispositivo é obrigatório" });
      }
      const clash = await Device.findOne({
        _id: { $ne: req.params.id },
        device_id: trimmed,
        isActive: true,
      });
      if (clash) return res.status(409).json({ message: "Já existe um dispositivo com este ID" });
      updates.device_id = trimmed;
    }

    if (company_id !== undefined || bridge_id !== undefined) {
      const current = await Device.findById(req.params.id).lean();
      if (!current) return res.status(404).json({ message: "Dispositivo não encontrado" });

      const newCompanyId = company_id ?? current.company_id?.toString();
      const newBridgeId = bridge_id ?? current.bridge_id?.toString();

      const company = await Company.findById(newCompanyId);
      if (!company || !company.isActive) {
        return res.status(404).json({ message: "Empresa não encontrada" });
      }
      const bridge = await Bridge.findById(newBridgeId);
      if (!bridge || !bridge.isActive || bridge.company_id.toString() !== newCompanyId) {
        return res
          .status(404)
          .json({ message: "Ponte não encontrada ou não pertence à empresa especificada" });
      }
      updates.company_id = newCompanyId;
      updates.bridge_id = newBridgeId;
    }

    const $setNested = buildSetFromBody(req.body);
    Object.assign(updates, $setNested);
    updates.updatedAt = new Date();

    const device = await Device.findByIdAndUpdate(req.params.id, updates, {
      new: true,
      runValidators: true,
    })
      .populate("company_id", "name")
      .populate("bridge_id", "name");

    if (!device) return res.status(404).json({ message: "Dispositivo não encontrado" });

    res.json({
      ...device.toObject(),
      company_name: device.company_id?.name || "Empresa não encontrada",
      bridge_name: device.bridge_id?.name || "Ponte não encontrada",
    });
  } catch (error) {
    console.error("Error updating device:", error);
    res.status(500).json({ message: "Erro ao atualizar dispositivo", error: error.message });
  }
});

/* ------------------------------ PATCHes ------------------------------ */

// PATCH /devices-crud/:id/params  → atualiza apenas params_current
router.patch("/:id/params", async (req, res) => {
  try {
    const $set = buildSetFromBody({ params_current: req.body || {} });
    if (!Object.keys($set).length) {
      return res.status(400).json({ message: "Nenhum parâmetro para atualizar" });
    }
    $set.updatedAt = new Date();

    const device = await Device.findByIdAndUpdate(req.params.id, $set, {
      new: true,
      runValidators: true,
    })
      .populate("company_id", "name")
      .populate("bridge_id", "name");

    if (!device) return res.status(404).json({ message: "Dispositivo não encontrado" });

    res.json({
      ...device.toObject(),
      company_name: device.company_id?.name || "Empresa não encontrada",
      bridge_name: device.bridge_id?.name || "Ponte não encontrada",
    });
  } catch (error) {
    console.error("Error patching device params:", error);
    res.status(500).json({ message: "Erro ao atualizar parâmetros", error: error.message });
  }
});

// PATCH /devices-crud/:id/mode  → altera somente infos.modo_operacao
router.patch("/:id/mode", async (req, res) => {
  try {
    const mode = String(req.body?.modo_operacao || "").trim();
    if (!mode) return res.status(400).json({ message: "modo_operacao é obrigatório" });

    const device = await Device.findByIdAndUpdate(
      req.params.id,
      { "infos.modo_operacao": mode, updatedAt: new Date() },
      { new: true, runValidators: true }
    )
      .populate("company_id", "name")
      .populate("bridge_id", "name");

    if (!device) return res.status(404).json({ message: "Dispositivo não encontrado" });

    res.json({
      ...device.toObject(),
      company_name: device.company_id?.name || "Empresa não encontrada",
      bridge_name: device.bridge_id?.name || "Ponte não encontrada",
    });
  } catch (error) {
    console.error("Error patching device mode:", error);
    res.status(500).json({ message: "Erro ao atualizar modo de operação", error: error.message });
  }
});

/* ------------------------------ DELETE ------------------------------ */

// DELETE /devices-crud/:id  (soft delete)
router.delete("/:id", async (req, res) => {
  try {
    const device = await Device.findByIdAndUpdate(
      req.params.id,
      { isActive: false, updatedAt: new Date() },
      { new: true }
    );
    if (!device) return res.status(404).json({ message: "Dispositivo não encontrado" });

    res.json({ message: "Dispositivo removido com sucesso" });
  } catch (error) {
    console.error("Error deleting device:", error);
    res.status(500).json({ message: "Erro ao remover dispositivo", error: error.message });
  }
});

export default router;