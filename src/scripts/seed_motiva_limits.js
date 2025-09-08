
import mongoose from "mongoose";
import Company from "../models/company.js";
import Bridge from "../models/bridge.js";
import Device from "../models/device.js";
import Recipient from "../models/recipient.js";
import BridgeLimit from "../models/bridgeLimit.js";

const MONGO_URI = process.env.MONGO_URI || "mongodb+srv://monitpontes:pontesfoda2024@monitoramentoccr.rfcinhq.mongodb.net/Monitoramento?retryWrites=true&w=majority&appName=MonitoramentoCCR";

async function main() {
  await mongoose.connect(MONGO_URI);
  console.log("Connected", MONGO_URI);

  const comp = await Company.findOneAndUpdate(
    { name: "Motiva" }, { $set: { name: "Motiva", isActive: true } },
    { upsert: true, new: true }
  );

  const bridgeName = "OAE km 54+313 (SP-348 - Rodovia dos Bandeirantes)";
  const bridge = await Bridge.findOneAndUpdate(
    { company_id: comp._id, name: bridgeName },
    { $set: { company_id: comp._id, name: bridgeName, code: "SP-348", isActive: true } },
    { upsert: true, new: true }
  );

  // limits: freq 4Hz, accel 2 m/s² (ajuste no dashboard depois)
  await BridgeLimit.findOneAndUpdate(
    { bridge_id: bridge._id },
    { $set: { company_id: comp._id, bridge_id: bridge._id, accel_alert: 10.0, accel_crftical: 12.0, freq_alert: 3.7, freq_critical: 7.0 } },
    { upsert: true, new: true }
  );

  // devices sim_sensor_01..05
  const baseParams = {
    intervalo_aquisicao: 1000, amostras: 4096, freq_amostragem: 50,
    activity_threshold: 0.9, modo_operacao: "acel", modo_execucao: "debug", modo_teste: "completo"
  };
  for (let i=1;i<=5;i++){
    const device_id = `sim_sensor_${String(i).padStart(2,"0")}`;
    await Device.findOneAndUpdate(
      { device_id },
      { $set: { device_id, company_id: comp._id, bridge_id: bridge._id, type: "aceleracao", params_current: baseParams, isActive: true } },
      { upsert: true, new: true, setDefaultsOnInsert: true }
    );
  }

  // recipient with phone (from the user)
  await Recipient.findOneAndUpdate(
    { bridge_id: bridge._id, phone: "+5511990187261" },
    { $set: {
      company_id: comp._id, bridge_id: bridge._id, name: "Responsável OAE",
      phone: "+5511990187261", channels: ["sms","push"], severity: ["warning","critical"], active: true
    } },
    { upsert: true, new: true }
  );

  console.log("Seed complete.");
  await mongoose.disconnect();
}

main().catch(e => { console.error(e); process.exit(1); });
