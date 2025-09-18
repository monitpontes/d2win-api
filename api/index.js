// api/index.js
import handler, { boot } from "../src/app.js";

// garante conexão no cold start
await boot();

export default handler; // <-- exporta a função default do app
