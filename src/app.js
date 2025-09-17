// src/app.js
import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import morgan from "morgan";
import path from "path";
import { fileURLToPath } from "url";

import { connectMongo } from "./lib/db.js";

// rotas antigas
import devicesRouter from "./routes/devices.js";
import ingestRouter from "./routes/ingest.js";
import legacyRouter from "./routes/legacy.js";
import recipientsRouter from "./routes/recipients.js";
import pushRouter from "./routes/push.js";
import alertsRouter from "./routes/alerts.js";
import telemetryRoutes from "./routes/telemetry.js";
import health from "./routes/health.js";

// CRUD novas
import companiesRouter from "./routes/companies.js";
import bridgesRouter from "./routes/bridges.js";
import bridgeLimitsRouter from "./routes/bridgeLimits.js";
import devicesCrudRouter from "./routes/devicesCrud.js";
import bridgeStatusRouter from "./routes/bridgeStatus.js";

// middlewares/serviços
import { errorHandler, notFound } from "./middleware/errors.js";
import { ensureTimeSeries } from "./scripts/initTimeseries.js";
import { startBridgeHeartbeat } from "./services/bridgeHeartbeat.js";

dotenv.config();

export const app = express();

app.use(cors());
app.use(express.json({ limit: "1mb" }));
app.use(morgan(process.env.LOG_LEVEL || "dev"));

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use("/public", express.static(path.join(__dirname, "public")));

// ---- Rotas ----
app.use("/devices", devicesRouter);
app.use("/ingest", ingestRouter);
app.use("/sensors", legacyRouter); // compat
app.use("/recipients", recipientsRouter);
app.use("/push", pushRouter);
app.use("/alerts", alertsRouter);
app.use(health);
app.use("/telemetry", telemetryRoutes);

// CRUD
app.use("/companies", companiesRouter);
app.use("/bridges", bridgesRouter);
app.use("/bridge-limits", bridgeLimitsRouter);
app.use("/devices-crud", devicesCrudRouter);
app.use("/bridge-status", bridgeStatusRouter);

// 404 + error
app.use(notFound);
app.use(errorHandler);

// ---- Boot (conexão e inicializações) ----
export async function boot() {
  const MONGO_URI = process.env.MONGO_URI;
  if (!MONGO_URI) throw new Error("Missing MONGO_URI");

  await connectMongo(MONGO_URI);

  if ((process.env.INIT_TIMESERIES || "false").toLowerCase() === "true") {
    await ensureTimeSeries();
    startBridgeHeartbeat();
  }
}

// ---- Start local (NÃO roda no Vercel) ----
if (!process.env.VERCEL && !process.env.NOW_REGION) {
  const PORT = process.env.PORT || 4000;
  boot()
    .then(() => {
      app.listen(PORT, () => console.log(`API listening on :${PORT}`));
    })
    .catch((e) => {
      console.error("Fatal boot error:", e);
      process.exit(1);
    });
}
