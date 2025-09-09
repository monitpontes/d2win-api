// src/lib/time.js
// Funções utilitárias para lidar com data/hora

// 1) Agora em ISO (UTC)
export function nowISO() {
  return new Date().toISOString(); // UTC
}

// 2) Agora em “ISO Brasil” (UTC-3, sem 'Z')
export function nowBrazilISO() {
  const date = new Date();
  const offsetMs = -3 * 60 * 60 * 1000; // UTC-3
  return new Date(date.getTime() + offsetMs).toISOString().slice(0, -1);
}

// 3) NOVO: Converte um Date(UTC) para “ISO Brasil” (sem 'Z')
export function toBrazilISOFromUTC(dateUTC) {
  const offsetMs = -3 * 60 * 60 * 1000; // UTC-3
  return new Date(dateUTC.getTime() + offsetMs).toISOString().slice(0, -1);
}

// 4) NOVO: Particiona em ts_br / date_br / hour_br a partir de um Date(UTC)
export function brazilPartsFromUTC(dateUTC) {
  const ts_br = toBrazilISOFromUTC(dateUTC);
  return {
    ts_br,
    date_br: ts_br.slice(0, 10),          // "YYYY-MM-DD"
    hour_br: Number(ts_br.slice(11, 13)), // 0..23
  };
}
