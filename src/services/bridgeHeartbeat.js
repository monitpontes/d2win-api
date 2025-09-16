// src/services/bridgeHeartbeat.js
import Device from "../models/device.js";
import BridgeStatus from "../models/bridgeStatus.js";
import { toBrazilISOFromUTC, brazilPartsFromUTC } from "../lib/time.js";

// janelas e intervalo (ajustáveis por .env)
const INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS || 60_000);   // roda a cada 60s
const ACTIVE_MS = Number(process.env.HEARTBEAT_ACTIVE_MS || 120_000);  // ativo se falou <= 2min
const STALE_MS = Number(process.env.HEARTBEAT_STALE_MS || 600_000);  // stale se falou <= 10min

function classify(ms) {
    if (ms <= ACTIVE_MS) return "active";
    if (ms <= STALE_MS) return "stale";
    return "offline";
}

export async function runBridgeHeartbeatOnce() {
    const now = Date.now();
    const devices = await Device.find(
        {},
        { device_id: 1, company_id: 1, bridge_id: 1, last_seen: 1, isActive: 1 }
    ).populate('company_id', 'name')
    .populate('bridge_id', 'name')
    .lean();

    // agrupa por ponte
    const byBridge = new Map();
    for (const d of devices) {
        const key = String(d.bridge_id._id || d.bridge_id);
        if (!byBridge.has(key)) {
            byBridge.set(key, { 
                company_id: d.company_id._id || d.company_id, 
                bridge_id: d.bridge_id._id || d.bridge_id,
                company_name: d.company_id?.name || 'Nome não encontrado',
                bridge_name: d.bridge_id?.name || 'Nome não encontrado',
                devices: [] 
            });
        }
        const last = d.last_seen ? new Date(d.last_seen) : null;
        const ms = last ? (now - last.getTime()) : Number.POSITIVE_INFINITY;
        const status = d.isActive === false ? "offline" : classify(ms);

        byBridge.get(key).devices.push({
            device_id: d.device_id,
            last_seen: last,
            ts_br: last ? toBrazilISOFromUTC(last) : null,
            ms_since: Number.isFinite(ms) ? ms : null,
            status
        });
    }

    // upsert 1 documento por ponte
    let processed = 0;
    for (const [, grp] of byBridge) {
        const totals = { total: grp.devices.length, active: 0, stale: 0, offline: 0 };
        for (const s of grp.devices) totals[s.status]++;
        const bridgeStatus =
            totals.active > 0 ? "active" :
                totals.stale > 0 ? "stale" : "offline";

        const nowUTC = new Date();
        const parts = brazilPartsFromUTC(nowUTC); // { ts_br, date_br, hour_br }

        await BridgeStatus.updateOne(
            { company_id: grp.company_id, bridge_id: grp.bridge_id },
            {
                $set: {
                    updated_at: nowUTC,
                    ts_br: parts.ts_br,
                    date_br: parts.date_br,
                    hour_br: parts.hour_br,
                    meta: {
                        company_name: grp.company_name,
                        bridge_name: grp.bridge_name
                    },
                    summary: { ...totals, status: bridgeStatus },
                    devices: grp.devices
                }
            },
            { upsert: true }
        );
        processed++;
    }
    return { ok: true, bridges_processed: processed, interval_ms: INTERVAL_MS, active_ms: ACTIVE_MS, stale_ms: STALE_MS };
}

let timer = null;
export function startBridgeHeartbeat() {
    if (String(process.env.HEARTBEAT_ENABLED || "true") !== "true") return;
    if (timer) return;
    // roda uma vez no boot e depois no intervalo
    setTimeout(() => { runBridgeHeartbeatOnce().catch(console.error); }, 10_000);
    timer = setInterval(() => { runBridgeHeartbeatOnce().catch(console.error); }, INTERVAL_MS);
    console.log(`[HB] Bridge heartbeat iniciado a cada ${INTERVAL_MS}ms (active<=${ACTIVE_MS}ms, stale<=${STALE_MS}ms).`);
}

export function stopBridgeHeartbeat() {
    if (timer) clearInterval(timer);
    timer = null;
}
