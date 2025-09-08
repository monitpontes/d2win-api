//index.js
import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import morgan from "morgan";
import path from "path";
import { fileURLToPath } from "url";
import { connectMongo } from "./lib/db.js";
import devicesRouter from "./routes/devices.js";
import ingestRouter from "./routes/ingest.js";
import legacyRouter from "./routes/legacy.js";
import recipientsRouter from "./routes/recipients.js";
import pushRouter from "./routes/push.js";
import alertsRouter from "./routes/alerts.js";
import { errorHandler, notFound } from "./middleware/errors.js";
import { ensureTimeSeries } from "./scripts/initTimeseries.js";
import health from "./routes/health.js";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));
app.use(morgan(process.env.LOG_LEVEL || "dev"));

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use("/public", express.static(path.join(__dirname, "public")));

// routes
app.use("/devices", devicesRouter);
app.use("/ingest", ingestRouter);
app.use("/sensors", legacyRouter); // backward-compat if needed
app.use("/recipients", recipientsRouter);
app.use("/push", pushRouter);
app.use("/alerts", alertsRouter);
app.use(health);

// 404 + error
app.use(notFound);
app.use(errorHandler);

const PORT = process.env.PORT || 4000;
const MONGO_URI = process.env.MONGO_URI;

if (!MONGO_URI) {
  console.error("Missing MONGO_URI in .env");
  process.exit(1);
}

const start = async () => {
  await connectMongo(MONGO_URI);
  if ((process.env.INIT_TIMESERIES || "false").toLowerCase() === "true") {
    await ensureTimeSeries();
  }
  app.listen(PORT, () => console.log(`API listening on :${PORT}`));
};

start().catch((e) => {
  console.error("Fatal boot error:", e);
  process.exit(1);
});
