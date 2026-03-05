import "dotenv/config";
import mongoose from "mongoose";
import { exportD5ToS3 } from "../services/export/exportDayD5.js";

const MONGO_URI = process.env.MONGO_URI;

async function main() {

  await mongoose.connect(MONGO_URI);
  const db = mongoose.connection.db;

  console.log("Mongo conectado");

  const result = await exportD5ToS3(db, {
    offsetDays: 3 // hoje 04/03 → exporta 01/03
  });

  console.log("RESULTADO:");
  console.log(result);

  process.exit(0);
}

main();