// src/routes/s3Test.js
import express from "express";
import { listPrefix, getObjectBuffer } from "../services/s3Objects.js";

const router = express.Router();

/**
 * GET /api/s3/prefix?prefix=telemetry_accel/raw/2025/10/
 * Lista objetos dentro do prefix.
 */
router.get("/prefix", async (req, res) => {
  try {
    const prefix = req.query.prefix;
    if (!prefix) return res.status(400).json({ error: "prefix é obrigatório" });

    const items = await listPrefix(prefix);
    res.json({ prefix, count: items.length, items });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

/**
 * GET /api/s3/object?key=telemetry_accel/raw/2025/10/accel_2025-10.parquet
 * Baixa o objeto (para teste). NÃO use em produção para arquivos grandes.
 */
router.get("/object", async (req, res) => {
  try {
    const key = req.query.key;
    if (!key) return res.status(400).json({ error: "key é obrigatório" });

    const buf = await getObjectBuffer(key);

    // devolve como download
    res.setHeader("Content-Type", "application/octet-stream");
    res.setHeader("Content-Disposition", `attachment; filename="${key.split("/").pop()}"`);
    res.send(buf);
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

export default router;