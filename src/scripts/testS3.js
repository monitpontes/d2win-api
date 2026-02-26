import "dotenv/config";
import { uploadTestFile } from "../src/services/s3Service.js";

async function run() {
  try {
    await uploadTestFile();
    console.log("Upload feito com sucesso 🚀");
  } catch (err) {
    console.error("Erro:", err);
  }
}

run();