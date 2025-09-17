// src/routes/auth.js
const router = require("express").Router()
const jwt = require("jsonwebtoken")
const bcrypt = require("bcryptjs")
const User = require("../models/user")

const JWT_SECRET = process.env.JWT_SECRET || "devsecret"

router.post("/login", async (req, res) => {
  const { email, password } = req.body
  const user = await User.findOne({ email })
  if (!user || !user.isActive) return res.status(401).json({ message: "Credenciais inválidas" })
  const ok = await bcrypt.compare(password, user.passwordHash)
  if (!ok) return res.status(401).json({ message: "Credenciais inválidas" })
  user.lastLogin = new Date()
  await user.save()
  const token = jwt.sign({ uid: user._id }, JWT_SECRET, { expiresIn: "7d" })
  res.json({ token, user })
})

router.get("/me", async (req, res) => {
  const hdr = req.headers.authorization || ""
  const token = hdr.startsWith("Bearer ") ? hdr.slice(7) : null
  if (!token) return res.status(401).json({ message: "Sem token" })
  try {
    const payload = jwt.verify(token, JWT_SECRET)
    const user = await User.findById(payload.uid)
    if (!user) return res.status(401).json({ message: "Inválido" })
    res.json(user)
  } catch {
    res.status(401).json({ message: "Inválido" })
  }
})

module.exports = router
