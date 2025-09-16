import { runBridgeHeartbeatOnce } from '../src/services/bridgeHeartbeat.js';
import { connectMongo } from '../src/lib/db.js';

// ADICIONAR ESTES IMPORTS:
import '../src/models/company.js';
import '../src/models/bridge.js';
import '../src/models/device.js';
import '../src/models/bridgeStatus.js';

export default async function handler(req, res) {
  try {
    await connectMongo(process.env.MONGO_URI);
    const result = await runBridgeHeartbeatOnce();
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}
