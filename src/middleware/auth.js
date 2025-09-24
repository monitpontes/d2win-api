// src/middleware/auth.js
import jwt from "jsonwebtoken";

const JWT_SECRET = process.env.JWT_SECRET || "devsecret";

export function authenticateToken(req, res, next) {
  const authHeader = req.headers.authorization;
  const token = authHeader && authHeader.split(" ")[1];

  if (token == null) return res.status(401).json({ message: "Token não fornecido" });

  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) return res.status(403).json({ message: "Token inválido" });
    req.user = user;
    next();
  });
}