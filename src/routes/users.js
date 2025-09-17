import express from "express";
import bcrypt from "bcryptjs";
import User from "../models/user.js";

const router = express.Router();

// GET /users?company_id=...
router.get("/", async (req, res) => {
  try {
    const { company_id } = req.query;
    const q = company_id ? { company_id } : {};
    const users = await User.find(q).sort({ createdAt: -1 });
    // sem passwordHash
    const list = users.map((u) => {
      const obj = u.toObject();
      delete obj.passwordHash;
      return obj;
    });
    return res.json(list);
  } catch (err) {
    console.error("Users/list error:", err);
    return res.status(500).json({ message: "Erro ao listar usuários" });
  }
});

// POST /users
router.post("/", async (req, res) => {
  try {
    const { name, email, password, role = "viewer", company_id } = req.body || {};
    if (!name || !email || !password) {
      return res.status(400).json({ message: "Nome, email e senha são obrigatórios" });
    }
    const exists = await User.findOne({ email });
    if (exists) return res.status(409).json({ message: "Email já cadastrado" });

    const passwordHash = await bcrypt.hash(password, 10);
    const u = await User.create({ name, email, passwordHash, role, company_id, isActive: true });

    const obj = u.toObject();
    delete obj.passwordHash;
    return res.status(201).json(obj);
  } catch (err) {
    console.error("Users/create error:", err);
    return res.status(500).json({ message: "Erro ao criar usuário" });
  }
});

// PUT /users/:id
router.put("/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, role, isActive, company_id, password } = req.body || {};

    const update = { name, role, isActive, company_id };
    if (password) {
      update.passwordHash = await bcrypt.hash(password, 10);
    }

    const u = await User.findByIdAndUpdate(id, update, { new: true });
    if (!u) return res.status(404).json({ message: "Usuário não encontrado" });

    const obj = u.toObject();
    delete obj.passwordHash;
    return res.json(obj);
  } catch (err) {
    console.error("Users/update error:", err);
    return res.status(500).json({ message: "Erro ao atualizar usuário" });
  }
});

// DELETE /users/:id
router.delete("/:id", async (req, res) => {
  try {
    const { id } = req.params;
    await User.findByIdAndDelete(id);
    return res.status(204).end();
  } catch (err) {
    console.error("Users/delete error:", err);
    return res.status(500).json({ message: "Erro ao remover usuário" });
  }
});

export default router;
