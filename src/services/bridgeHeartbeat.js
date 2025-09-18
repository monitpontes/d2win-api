// ... (imports já existentes)
import Bridge from "../models/bridge.js";
import Device from "../models/device.js";
import BridgeStatus from "../models/bridgeStatus.js";

const EVERY_MS = Number(process.env.BRIDGE_HB_EVERY_MS || 60_000);
const ACTIVE_MS = Number(process.env.DEVICE_ACTIVE_MS || 120_000);
const STALE_MS  = Number(process.env.DEVICE_STALE_MS  || 600_000);

// Esta função faz UM ciclo de atualização e retorna estatísticas
export async function runBridgeHeartbeatOnce() {
  const now = Date.now();

  // pegue as pontes ativas
  const bridges = await Bridge.find({ isActive: true }).lean();

  let processed = 0;

  for (const br of bridges) {
    // dispositivos da ponte
    const devs = await Device.find({ isActive: true, bridge_id: br._id }).lean();

    const summary = {
      total: devs.length,
      active: 0,
      stale: 0,
      offline: 0,
      status: "offline",
    };

    for (const d of devs) {
      const last = new Date(d.last_seen || d.updatedAt || 0).getTime();
      const age = now - last;

      if (age <= ACTIVE_MS) {
        summary.active += 1;
      } else if (age <= STALE_MS) {
        summary.stale += 1;
      } else {
        summary.offline += 1;
      }
    }

    // status geral da ponte
    if (summary.active > 0) summary.status = "active";
    else if (summary.stale > 0) summary.status = "stale";
    else summary.status = "offline";

    await BridgeStatus.findOneAndUpdate(
      { bridge_id: br._id },
      {
        bridge_id: br._id,
        company_id: br.company_id,
        devices: devs.map((d) => ({
          _id: d._id,
          device_id: d.device_id,
          last_seen: d.last_seen || d.updatedAt,
        })),
        summary,
        ts_br: new Date(now),
        updated_at: new Date(now),
      },
      { upsert: true, new: true, setDefaultsOnInsert: true }
    );

    processed += 1;
  }

  return { processed, interval_ms: EVERY_MS, active_ms: ACTIVE_MS, stale_ms: STALE_MS };
}

// versão “timer” para ambientes NÃO-serverless (dev/VM)
export function startBridgeHeartbeat() {
  setInterval(runBridgeHeartbeatOnce, EVERY_MS);
  runBridgeHeartbeatOnce().catch(() => {});
}
