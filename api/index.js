// api/index.js
import app, { boot } from "../src/app.js";
import handler from "./heartbeat.js";

// Garante que conecta no banco antes de responder
await boot();

export default handler;
