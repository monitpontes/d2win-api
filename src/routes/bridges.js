// // src/routes/bridges.js
// import express from "express";
// import Bridge from "../models/bridge.js";
// import Company from "../models/company.js";
// import Device from "../models/device.js"; // <— ADICIONE ISSO

// const router = express.Router();

// /** ===== Helpers para gerar IDs sem precisar de abbr persistido ===== **/
// function sanitizeNameToAbbr(name, max = 12) {
//   if (!name) return "";
//   const clean = String(name)
//     .normalize("NFD").replace(/[\u0300-\u036f]/g, "") // tira acento
//     .replace(/[^A-Za-z0-9\s]/g, " ")                 // tira símbolos
//     .replace(/\s+/g, " ")                            // normaliza espaços
//     .trim();
//   if (!clean) return "";
//   const parts = clean.split(" ").filter(Boolean);
//   if (parts.length === 1) return parts[0].toUpperCase().slice(0, max);
//   // várias palavras → iniciais (ex.: "Ponte Rio-Niterói" => "PRN")
//   return parts.map(p => p[0]).join("").toUpperCase().slice(0, max);
// }
// function pad2(n) { return String(n).padStart(2, "0"); }

// /**
//  * Cria devices para a ponte recém-criada sem duplicar.
//  * Formato: EMP_ABBR_BRIDGE_ABBR_S01..SNN (ex.: MOTIVA_OAE2_S01)
//  */
// async function createDevicesForBridge({ company, bridge, sensorsCount = 5 }) {
//   const compAbbr   = sanitizeNameToAbbr(company.name, 12);
//   const bridgeAbbr = sanitizeNameToAbbr(bridge.name, 16);
//   const prefix = `${compAbbr}_${bridgeAbbr}_S`;

//   // Descobre quais já existem com o mesmo prefixo (para não duplicar)
//   const existing = await Device.find({
//     isActive: true,
//     company_id: company._id,
//     bridge_id: bridge._id,
//     device_id: { $regex: `^${prefix}\\d{2}$`, $options: "i" },
//   }).select("device_id");

//   const existingSet = new Set(existing.map(d => d.device_id.toUpperCase()));

//   const docs = [];
//   for (let i = 1; i <= sensorsCount; i++) {
//     const id = `${prefix}${pad2(i)}`; // MOTIVA_OAE2_S01
//     if (existingSet.has(id.toUpperCase())) continue;

//     // Defaults alinhados aos nomes da sua API
//     docs.push({
//       device_id: id,
//       company_id: company._id,
//       bridge_id: bridge._id,
//       isActive: true,
//       last_seen: null,
//       meta: { location: bridge.location || "", axis: "z" },

//       // valores iniciais compatíveis com seu payload (pode ajustar)
//       params_current: {
//         modo_teste: "completo",
//         tempo_calibracao: 5000,
//         intervalo_aquisicao: 1000,
//         amostras: 4096,
//         freq_amostragem: 2000,
//         activity_threshold: 1,
//         modo_execucao: "debug",        // "debug" | "operacao" | "teste"
//       },
//       infos: {
//         dados_pendentes: 0,
//         sDOK: true,
//         restartPending: false,
//         firmware_version: "3.4.0_autoregister",
//         modo_operacao: "aceleracao",   // "aceleracao" | "frequencia"
//       },
//     });
//   }

//   if (docs.length) {
//     await Device.insertMany(docs, { ordered: false });
//   }
// }

// /** ========================= ROTAS ========================= **/

// // GET /bridges
// router.get("/", async (req, res) => {
//   try {
//     const { company_id } = req.query;
//     const filter = { isActive: true };
//     if (company_id) filter.company_id = company_id;

//     const bridges = await Bridge.find(filter)
//       .populate("company_id", "name")
//       .sort({ name: 1 })
//       .lean();

//     const bridgesWithNames = bridges.map(b => ({
//       ...b,
//       company_name: b.company_id?.name || "Empresa não encontrada",
//     }));

//     res.json(bridgesWithNames);
//   } catch (error) {
//     console.error("Error fetching bridges:", error);
//     res.status(500).json({ message: "Erro ao buscar pontes", error: error.message });
//   }
// });

// // GET /bridges/:id
// router.get("/:id", async (req, res) => {
//   try {
//     const bridge = await Bridge.findById(req.params.id)
//       .populate("company_id", "name")
//       .lean();

//     if (!bridge) return res.status(404).json({ message: "Ponte não encontrada" });

//     res.json({
//       ...bridge,
//       company_name: bridge.company_id?.name || "Empresa não encontrada",
//     });
//   } catch (error) {
//     console.error("Error fetching bridge:", error);
//     res.status(500).json({ message: "Erro ao buscar ponte", error: error.message });
//   }
// });

// // POST /bridges  (cria ponte + devices automáticos)
// router.post("/", async (req, res) => {
//   try {
//     const { name, company_id, location, description, sensors_count } = req.body;

//     if (!name?.trim())   return res.status(400).json({ message: "Nome da ponte é obrigatório" });
//     if (!company_id)     return res.status(400).json({ message: "ID da empresa é obrigatório" });

//     const company = await Company.findById(company_id);
//     if (!company || !company.isActive) {
//       return res.status(404).json({ message: "Empresa não encontrada" });
//     }

//     const existingBridge = await Bridge.findOne({
//       name: { $regex: new RegExp(`^${name.trim()}$`, "i") },
//       company_id,
//       isActive: true,
//     });
//     if (existingBridge) {
//       return res.status(409).json({ message: "Já existe uma ponte com este nome nesta empresa" });
//     }

//     const bridge = new Bridge({
//       name: name.trim(),
//       company_id,
//       location: location?.trim() || "",
//       description: description?.trim() || "",
//       isActive: true,
//     });

//     const savedBridge = await bridge.save();

//     // CRIA DEVICES (não falha a criação da ponte se der erro)
//     try {
//       const count = Number(sensors_count) > 0 ? Number(sensors_count) : 5;
//       await createDevicesForBridge({ company, bridge: savedBridge, sensorsCount: count });
//     } catch (e) {
//       console.warn("Falha ao criar devices automáticos:", e?.message);
//     }

//     const populatedBridge = await Bridge.findById(savedBridge._id)
//       .populate("company_id", "name")
//       .lean();

//     res.status(201).json({
//       ...populatedBridge,
//       company_name: populatedBridge.company_id?.name || "Empresa não encontrada",
//     });
//   } catch (error) {
//     console.error("Error creating bridge:", error);
//     res.status(500).json({ message: "Erro ao criar ponte", error: error.message });
//   }
// });

// // PUT /bridges/:id
// router.put("/:id", async (req, res) => {
//   try {
//     const { name, company_id, location, description } = req.body;

//     if (!name?.trim())   return res.status(400).json({ message: "Nome da ponte é obrigatório" });
//     if (!company_id)     return res.status(400).json({ message: "ID da empresa é obrigatório" });

//     const company = await Company.findById(company_id);
//     if (!company || !company.isActive) {
//       return res.status(404).json({ message: "Empresa não encontrada" });
//     }

//     const existingBridge = await Bridge.findOne({
//       _id: { $ne: req.params.id },
//       name: { $regex: new RegExp(`^${name.trim()}$`, "i") },
//       company_id,
//       isActive: true,
//     });
//     if (existingBridge) {
//       return res.status(409).json({ message: "Já existe uma ponte com este nome nesta empresa" });
//     }

//     const bridge = await Bridge.findByIdAndUpdate(
//       req.params.id,
//       {
//         name: name.trim(),
//         company_id,
//         location: location?.trim() || "",
//         description: description?.trim() || "",
//         updatedAt: new Date(),
//       },
//       { new: true, runValidators: true }
//     ).populate("company_id", "name");

//     if (!bridge) return res.status(404).json({ message: "Ponte não encontrada" });

//     res.json({
//       ...bridge.toObject(),
//       company_name: bridge.company_id?.name || "Empresa não encontrada",
//     });
//   } catch (error) {
//     console.error("Error updating bridge:", error);
//     res.status(500).json({ message: "Erro ao atualizar ponte", error: error.message });
//   }
// });

// // DELETE /bridges/:id  (soft delete)  + (opcional) cascade nos devices
// router.delete("/:id", async (req, res) => {
//   try {
//     const bridge = await Bridge.findByIdAndUpdate(
//       req.params.id,
//       { isActive: false, updatedAt: new Date() },
//       { new: true }
//     );
//     if (!bridge) return res.status(404).json({ message: "Ponte não encontrada" });

//     // (opcional) também desativar devices vinculados
//     try {
//       await Device.updateMany(
//         { bridge_id: bridge._id, isActive: true },
//         { $set: { isActive: false, "infos.modo_operacao": "aceleracao" }, $currentDate: { updatedAt: true } }
//       );
//     } catch (e) {
//       console.warn("Falha ao desativar devices da ponte:", e?.message);
//     }

//     res.json({ message: "Ponte removida com sucesso" });
//   } catch (error) {
//     console.error("Error deleting bridge:", error);
//     res.status(500).json({ message: "Erro ao remover ponte", error: error.message });
//   }
// });

// export default router;

// src/routes/bridges.js
import express from "express";
import Bridge from "../models/bridge.js";
import Company from "../models/company.js";
import Device from "../models/device.js";

// === KML -> GeoJSON ===
import multer from "multer";
import { DOMParser } from "xmldom";
import { kml } from "@tmcw/togeojson";

const router = express.Router();

/** ===== Helpers para gerar IDs sem precisar de abbr persistido ===== **/
function sanitizeNameToAbbr(name, max = 12) {
  if (!name) return "";
  const clean = String(name)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // tira acento
    .replace(/[^A-Za-z0-9\s]/g, " ") // tira símbolos
    .replace(/\s+/g, " ") // normaliza espaços
    .trim();
  if (!clean) return "";
  const parts = clean.split(" ").filter(Boolean);
  if (parts.length === 1) return parts[0].toUpperCase().slice(0, max);
  // várias palavras -> iniciais
  return parts
    .map((p) => p[0])
    .join("")
    .toUpperCase()
    .slice(0, max);
}
function pad2(n) {
  return String(n).padStart(2, "0");
}
function isFiniteNumber(n) {
  return typeof n === "number" && Number.isFinite(n);
}

/**
 * Normaliza payload antigo/novo para o novo schema.
 * Aceita:
 * - location (string) -> locationText
 * - location {lat,lng} -> coordinates
 * - coordinates -> coordinates
 */
function normalizeBridgeInput(body) {
  const out = {};

  // básicos
  if (body.name !== undefined) out.name = String(body.name).trim();
  if (body.company_id !== undefined) out.company_id = body.company_id;
  if (body.code !== undefined) out.code = String(body.code).trim();
  if (body.tags !== undefined) out.tags = Array.isArray(body.tags) ? body.tags : [];
  if (body.isActive !== undefined) out.isActive = !!body.isActive;

  // locationText novo ou location string antigo
  if (body.locationText !== undefined) {
    out.locationText = String(body.locationText).trim();
  } else if (typeof body.location === "string") {
    out.locationText = body.location.trim();
  }

  // coordinates novo
  if (body.coordinates && typeof body.coordinates === "object") {
    const lat = Number(body.coordinates.lat);
    const lng = Number(body.coordinates.lng);
    if (isFiniteNumber(lat) && isFiniteNumber(lng)) out.coordinates = { lat, lng };
  }

  // location {lat,lng} antigo
  if (!out.coordinates && body.location && typeof body.location === "object") {
    const lat = Number(body.location.lat);
    const lng = Number(body.location.lng);
    if (isFiniteNumber(lat) && isFiniteNumber(lng)) out.coordinates = { lat, lng };
  }

  // campos novos (strings)
  const optionalStrings = [
    "concession",
    "rodovia",
    "beamType",
    "spanType",
    "material",
    "lastMajorIntervention",
    "image",
    "geoReferencedImage",
    "impactNotes",
  ];
  for (const k of optionalStrings) {
    if (body[k] !== undefined) out[k] = String(body[k]).trim();
  }

  // campos novos (números)
  const optionalNumbers = ["km", "supportCount", "length", "width", "capacity", "constructionYear"];
  for (const k of optionalNumbers) {
    if (body[k] !== undefined && body[k] !== null && body[k] !== "") {
      const v = Number(body[k]);
      if (Number.isFinite(v)) out[k] = v;
    }
  }

  // enums
  if (body.typology !== undefined) out.typology = body.typology; // Ponte/Viaduto/Passarela
  if (body.criticality !== undefined) out.criticality = body.criticality; // low/medium/high

  // geojson (se você quiser atualizar via PUT também)
  if (body.geojson !== undefined) out.geojson = body.geojson;

  // ===== Editor 3D (Admin) =====
  // Permite salvar anotações 3D via PUT /bridges/:id
  if (body.annotations3d !== undefined) {
    out.annotations3d = Array.isArray(body.annotations3d) ? body.annotations3d : [];
  }

  return out;
}

/**
 * Cria devices para a ponte recém-criada sem duplicar.
 * Formato: EMP_ABBR_BRIDGE_ABBR_S01..SNN (ex.: MOTIVA_OAE2_S01)
 */
async function createDevicesForBridge({ company, bridge, sensorsCount = 5 }) {
  const compAbbr = sanitizeNameToAbbr(company.name, 12);
  const bridgeAbbr = sanitizeNameToAbbr(bridge.name, 16);
  const prefix = `${compAbbr}_${bridgeAbbr}_S`;

  // Descobre quais já existem com o mesmo prefixo (para não duplicar)
  const existing = await Device.find({
    isActive: true,
    company_id: company._id,
    bridge_id: bridge._id,
    device_id: { $regex: `^${prefix}\\d{2}$`, $options: "i" },
  }).select("device_id");

  const existingSet = new Set(existing.map((d) => d.device_id.toUpperCase()));

  const docs = [];
  for (let i = 1; i <= sensorsCount; i++) {
    const id = `${prefix}${pad2(i)}`; // MOTIVA_OAE2_S01
    if (existingSet.has(id.toUpperCase())) continue;

    docs.push({
      device_id: id,
      company_id: company._id,
      bridge_id: bridge._id,
      isActive: true,
      last_seen: null,
      meta: {
        location: bridge.locationText || "",
        coordinates: bridge.coordinates || null,
        axis: "z",
      },

      // valores iniciais compatíveis com seu payload (pode ajustar)
      params_current: {
        modo_teste: "completo",
        tempo_calibracao: 5000,
        intervalo_aquisicao: 1000,
        amostras: 4096,
        freq_amostragem: 2000,
        activity_threshold: 1,
        modo_execucao: "debug", // "debug" | "operacao" | "teste"
      },
      infos: {
        dados_pendentes: 0,
        sDOK: true,
        restartPending: false,
        firmware_version: "3.4.0_autoregister",
        modo_operacao: "aceleracao", // "aceleracao" | "frequencia"
      },
    });
  }

  if (docs.length) {
    await Device.insertMany(docs, { ordered: false });
  }
}

/** ========================= ROTAS ========================= **/

// GET /bridges
router.get("/", async (req, res) => {
  try {
    const { company_id } = req.query;
    const filter = { isActive: true };
    if (company_id) filter.company_id = company_id;

    const bridges = await Bridge.find(filter).populate("company_id", "name").sort({ name: 1 }).lean();

    const bridgesWithNames = bridges.map((b) => ({
      ...b,
      company_name: b.company_id?.name || "Empresa não encontrada",
    }));

    res.json(bridgesWithNames);
  } catch (error) {
    console.error("Error fetching bridges:", error);
    res.status(500).json({ message: "Erro ao buscar pontes", error: error.message });
  }
});

// GET /bridges/:id
router.get("/:id", async (req, res) => {
  try {
    const bridge = await Bridge.findById(req.params.id).populate("company_id", "name").lean();

    if (!bridge) return res.status(404).json({ message: "Ponte não encontrada" });

    res.json({
      ...bridge,
      company_name: bridge.company_id?.name || "Empresa não encontrada",
    });
  } catch (error) {
    console.error("Error fetching bridge:", error);
    res.status(500).json({ message: "Erro ao buscar ponte", error: error.message });
  }
});

// POST /bridges  (cria ponte + devices automáticos)
router.post("/", async (req, res) => {
  try {
    const { sensors_count } = req.body;
    const input = normalizeBridgeInput(req.body);

    if (!input.name?.trim()) return res.status(400).json({ message: "Nome da ponte é obrigatório" });
    if (!input.company_id) return res.status(400).json({ message: "ID da empresa é obrigatório" });

    const company = await Company.findById(input.company_id);
    if (!company || !company.isActive) {
      return res.status(404).json({ message: "Empresa não encontrada" });
    }

    const existingBridge = await Bridge.findOne({
      name: { $regex: new RegExp(`^${input.name.trim()}$`, "i") },
      company_id: input.company_id,
      isActive: true,
    });
    if (existingBridge) {
      return res.status(409).json({ message: "Já existe uma ponte com este nome nesta empresa" });
    }

    const bridge = new Bridge({
      ...input,
      isActive: input.isActive ?? true,
    });

    const savedBridge = await bridge.save();

    // CRIA DEVICES (não falha a criação da ponte se der erro)
    try {
      const count = Number(sensors_count) > 0 ? Number(sensors_count) : 5;
      await createDevicesForBridge({ company, bridge: savedBridge, sensorsCount: count });
    } catch (e) {
      console.warn("Falha ao criar devices automáticos:", e?.message);
    }

    const populatedBridge = await Bridge.findById(savedBridge._id).populate("company_id", "name").lean();

    res.status(201).json({
      ...populatedBridge,
      company_name: populatedBridge.company_id?.name || "Empresa não encontrada",
    });
  } catch (error) {
    console.error("Error creating bridge:", error);
    res.status(500).json({ message: "Erro ao criar ponte", error: error.message });
  }
});

// PUT /bridges/:id  (agora suporta update parcial, inclusive company_id + annotations3d)
router.put("/:id", async (req, res) => {
  try {
    const input = normalizeBridgeInput(req.body);

    // Busca ponte atual para permitir updates parciais
    const current = await Bridge.findById(req.params.id).lean();
    if (!current) return res.status(404).json({ message: "Ponte não encontrada" });

    // fallback para não exigir name/company_id em updates parciais
    const finalCompanyId = input.company_id ?? current.company_id?.toString();
    const finalName = input.name ?? current.name;

    if (!finalName?.trim()) return res.status(400).json({ message: "Nome da ponte é obrigatório" });
    if (!finalCompanyId) return res.status(400).json({ message: "ID da empresa é obrigatório" });

    const company = await Company.findById(finalCompanyId);
    if (!company || !company.isActive) {
      return res.status(404).json({ message: "Empresa não encontrada" });
    }

    const existingBridge = await Bridge.findOne({
      _id: { $ne: req.params.id },
      name: { $regex: new RegExp(`^${String(finalName).trim()}$`, "i") },
      company_id: finalCompanyId,
      isActive: true,
    });
    if (existingBridge) {
      return res.status(409).json({ message: "Já existe uma ponte com este nome nesta empresa" });
    }

    // garante consistência caso o frontend mande company_id + annotations3d sem name
    input.company_id = finalCompanyId;
    input.name = String(finalName).trim();

    const bridge = await Bridge.findByIdAndUpdate(
      req.params.id,
      { $set: { ...input }, $currentDate: { updatedAt: true } },
      { new: true, runValidators: true }
    ).populate("company_id", "name");

    if (!bridge) return res.status(404).json({ message: "Ponte não encontrada" });

    res.json({
      ...bridge.toObject(),
      company_name: bridge.company_id?.name || "Empresa não encontrada",
    });
  } catch (error) {
    console.error("Error updating bridge:", error);
    res.status(500).json({ message: "Erro ao atualizar ponte", error: error.message });
  }
});

// DELETE /bridges/:id  (soft delete)  + (opcional) cascade nos devices
router.delete("/:id", async (req, res) => {
  try {
    const bridge = await Bridge.findByIdAndUpdate(
      req.params.id,
      { $set: { isActive: false }, $currentDate: { updatedAt: true } },
      { new: true }
    );
    if (!bridge) return res.status(404).json({ message: "Ponte não encontrada" });

    // (opcional) também desativar devices vinculados
    try {
      await Device.updateMany(
        { bridge_id: bridge._id, isActive: true },
        { $set: { isActive: false, "infos.modo_operacao": "aceleracao" }, $currentDate: { updatedAt: true } }
      );
    } catch (e) {
      console.warn("Falha ao desativar devices da ponte:", e?.message);
    }

    res.json({ message: "Ponte removida com sucesso" });
  } catch (error) {
    console.error("Error deleting bridge:", error);
    res.status(500).json({ message: "Erro ao remover ponte", error: error.message });
  }
});

/** ========================= KML -> GEOJSON ========================= **/

// Upload em memória (sem salvar arquivo por enquanto)
const upload = multer({ storage: multer.memoryStorage() });

/**
 * POST /bridges/:id/upload-kml
 * form-data: file=<arquivo.kml>
 * Converte KML para GeoJSON e salva em bridge.geojson
 */
router.post("/:id/upload-kml", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ message: "Envie um arquivo .kml no campo 'file'." });
    }

    const bridge = await Bridge.findById(req.params.id);
    if (!bridge) return res.status(404).json({ message: "Ponte não encontrada" });

    // Converte buffer para texto
    const kmlText = req.file.buffer.toString("utf-8");

    // Parse do XML
    const dom = new DOMParser().parseFromString(kmlText, "text/xml");

    // KML -> GeoJSON (FeatureCollection)
    const geojson = kml(dom);

    // Salva no documento
    bridge.geojson = geojson;
    await bridge.save();

    return res.json({
      message: "GeoJSON salvo com sucesso.",
      geojson: bridge.geojson,
    });
  } catch (error) {
    console.error("Erro ao converter KML:", error);
    return res.status(500).json({ message: "Erro ao processar KML", error: error.message });
  }
});

export default router;