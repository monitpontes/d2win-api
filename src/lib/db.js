// src/lib/db.js
import mongoose from "mongoose";

const status = {
  state: "disconnected",        // disconnected | connecting | connected
  readyState: 0,                 // 0..3 do mongoose
  lastConnectedAt: null,
  lastError: null,
};

export const connectMongo = async (uri) => {
  if (mongoose.connection.readyState === 1) return; // já conectado
  mongoose.set("strictQuery", true);

  status.state = "connecting";
  status.readyState = mongoose.connection.readyState;

  // listeners (uma vez só)
  if (!mongoose.connection._healthHooked) {
    mongoose.connection._healthHooked = true;

    mongoose.connection.on("connected", () => {
      status.state = "connected";
      status.readyState = mongoose.connection.readyState;
      status.lastConnectedAt = new Date().toISOString();
      status.lastError = null;
      console.log("[DB] Mongo connected");
    });

    mongoose.connection.on("disconnected", () => {
      status.state = "disconnected";
      status.readyState = mongoose.connection.readyState;
      console.warn("[DB] Mongo disconnected");
    });

    mongoose.connection.on("error", (err) => {
      status.state = "disconnected";
      status.readyState = mongoose.connection.readyState;
      status.lastError = err?.message || String(err);
      console.error("[DB] Mongo error:", status.lastError);
    });
  }

  await mongoose.connect(uri);
};

export const db = () => mongoose.connection;

// helper para a rota de saúde
export const dbHealthSnapshot = async () => {
  try {
    // tenta um ping real
    await mongoose.connection.db.admin().command({ ping: 1 });
    return { ok: true, ...status };
  } catch (err) {
    return { ok: false, ...status, lastError: err?.message || String(err) };
  }
};
