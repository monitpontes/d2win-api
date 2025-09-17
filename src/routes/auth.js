import express from "express";
import jwt from "jsonwebtoken";
import bcrypt from "bcryptjs";
import User from "../models/user.js";

const router = express.Router();

const JWT_SECRET = process.env.JWT_SECRET || "devsecret";
const JWT_EXPIRES = process.env.JWT_EXPIRES || "7d";

// helper para esconder passwordHash
function sanitize(userDoc) {
  const u = userDoc.toObject ? userDoc.toObject() : { ...userDoc };
  delete u.passwordHash;
  return u;
}

// POST /auth/login
router.post("/login", async (req, res) => {
  try {
    const { email, password } = req.body || {};
    if (!email || !password) {
      return res.status(400).json({ message: "Email e senha são obrigatórios" });
    }

    const user = await User.findOne({ email });
    if (!user || !user.isActive) {
      return res.status(401).json({ message: "Credenciais inválidas" });
    }

    const ok = await bcrypt.compare(password, user.passwordHash);
    if (!ok) {
      return res.status(401).json({ message: "Credenciais inválidas" });
    }

    user.lastLogin = new Date();
    await user.save();

    const token = jwt.sign({ uid: user._id, role: user.role }, JWT_SECRET, { expiresIn: JWT_EXPIRES });
    return res.json({ token, user: sanitize(user) });
  } catch (err) {
    console.error("Auth/login error:", err);
    return res.status(500).json({ message: "Erro ao autenticar" });
  }
});

// GET /auth/me
router.get("/me", async (req, res) => {
  try {
    const hdr = req.headers.authorization || "";
    const token = hdr.startsWith("Bearer ") ? hdr.slice(7) : null;
    if (!token) return res.status(401).json({ message: "Sem token" });

    const payload = jwt.verify(token, JWT_SECRET);
    const user = await User.findById(payload.uid);
    if (!user) return res.status(401).json({ message: "Inválido" });

    return res.json(sanitize(user));
  } catch (err) {
    return res.status(401).json({ message: "Inválido" });
  }
});

// (Opcional) POST /auth/register — útil para testes/seed via API
router.post("/register", async (req, res) => {
  try {
    const { name, email, password, role = "viewer", company_id } = req.body || {};
    if (!name || !email || !password) {
      return res.status(400).json({ message: "Nome, email e senha são obrigatórios" });
    }
    const exists = await User.findOne({ email });
    if (exists) return res.status(409).json({ message: "Email já cadastrado" });

    const passwordHash = await bcrypt.hash(password, 10);
    const user = await User.create({ name, email, passwordHash, role, company_id, isActive: true });

    const token = jwt.sign({ uid: user._id, role: user.role }, JWT_SECRET, { expiresIn: JWT_EXPIRES });
    return res.status(201).json({ token, user: sanitize(user) });
  } catch (err) {
    console.error("Auth/register error:", err);
    return res.status(500).json({ message: "Erro ao registrar" });
  }
});

export default router;
