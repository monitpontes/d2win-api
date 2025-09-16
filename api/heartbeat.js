import { runBridgeHeartbeatOnce } from '../src/services/bridgeHeartbeat.js';
import { connectMongo } from '../src/lib/db.js';

export default async function handler(req, res) {
  try {
    await connectMongo(process.env.MONGO_URI);
    const result = await runBridgeHeartbeatOnce();
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
}
