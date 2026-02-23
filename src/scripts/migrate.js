import mongoose from "mongoose";
import dotenv from "dotenv";

dotenv.config();

const MONGO_URI = "mongodb+srv://monitpontes:pontesfoda2024@monitoramentoccr.rfcinhq.mongodb.net/Monitoramento?retryWrites=true&w=majority&appName=MonitoramentoCCR"

if (!MONGO_URI) {
  console.error("❌ MONGO_URI não definida no .env");
  process.exit(1);
}

async function run() {
  await mongoose.connect(MONGO_URI);
  console.log("✅ Conectado ao MongoDB");

  const col = mongoose.connection.collection("bridges");

  const cursor = col.find({});
  let updated = 0;
  let scanned = 0;

  while (await cursor.hasNext()) {
    const doc = await cursor.next();
    scanned++;

    const $set = {};
    let needsUpdate = false;

    function ensureField(path, value) {
      const keys = path.split(".");
      let current = doc;
      for (let i = 0; i < keys.length; i++) {
        if (current[keys[i]] === undefined) {
          $set[path] = value;
          needsUpdate = true;
          return;
        }
        current = current[keys[i]];
      }
    }

    // ===== Estrutura completa do Schema =====

    ensureField("tags", []);
    ensureField("isActive", true);

    ensureField("locationText", "");
    ensureField("coordinates", { lat: null, lng: null });

    ensureField("criticality", "low");
    ensureField("impactNotes", "");

    ensureField("concession", "");
    ensureField("rodovia", "");
    ensureField("km", null);

    ensureField("typology", "Ponte");
    ensureField("beamType", "");
    ensureField("spanType", "");
    ensureField("material", "");
    ensureField("supportCount", null);

    ensureField("length", null);
    ensureField("width", null);
    ensureField("capacity", null);

    ensureField("constructionYear", null);
    ensureField("lastMajorIntervention", "");

    ensureField("image", "");
    ensureField("geoReferencedImage", "");

    ensureField("geojson", undefined);

    if (needsUpdate) {
      $set.updatedAt = new Date();

      await col.updateOne(
        { _id: doc._id },
        { $set }
      );

      updated++;
    }
  }

  console.log(`✅ Finalizado.`);
  console.log(`📊 Documentos verificados: ${scanned}`);
  console.log(`🛠️ Documentos atualizados: ${updated}`);

  await mongoose.disconnect();
  console.log("🔌 Conexão encerrada");
}

run().catch(err => {
  console.error("❌ Erro na padronização:", err);
  process.exit(1);
});