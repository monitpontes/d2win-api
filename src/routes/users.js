// src/routes/users.js
const router = require("express").Router()
const bcrypt = require("bcryptjs")
const User = require("../models/user")

router.get("/", async (req, res) => {
  const { company_id } = req.query
  const q = company_id ? { company_id } : {}
  const users = await User.find(q).sort({ createdAt: -1 })
  res.json(users)
})

router.post("/", async (req, res) => {
  const { name, email, password, role = "viewer", company_id } = req.body
  if (!name || !email || !password) return res.status(400).json({ message: "Nome, email e senha são obrigatórios" })
  const passwordHash = await bcrypt.hash(password, 10)
  const u = await User.create({ name, email, passwordHash, role, company_id })
  res.status(201).json(u)
})

router.put("/:id", async (req, res) => {
  const { id } = req.params
  const { name, role, isActive, company_id } = req.body
  const u = await User.findByIdAndUpdate(id, { name, role, isActive, company_id }, { new: true })
  res.json(u)
})

router.delete("/:id", async (req, res) => {
  const { id } = req.params
  await User.findByIdAndDelete(id)
  res.status(204).end()
})

module.exports = router
