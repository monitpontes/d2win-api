// // src/lib/limits.js
// export function classifyAccel(value, warn, crit) {
//   if (Math.abs(value) >= crit)   return "critical";
//   if (Math.abs(value) >= warn)   return "warning";
//   return null;
// }

// export function classifyFreq(maxPeak, warn, crit) {
//   if (maxPeak >= crit)  return "critical";
//   if (maxPeak >= warn)  return "warning";
//   return null;
// }

// src/lib/limits.js
export function classifyTwoSided(value, minAlert, minCrit, maxAlert, maxCrit) {
  // máximos têm prioridade de severidade
  if (maxCrit  != null && value >= maxCrit)  return "critical";
  if (maxAlert != null && value >= maxAlert) return "warning";
  // mínimos (lado baixo)
  if (minCrit  != null && value <= minCrit)  return "critical";
  if (minAlert != null && value <= minAlert) return "warning";
  return "normal";
}

