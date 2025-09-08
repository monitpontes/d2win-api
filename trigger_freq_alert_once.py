
#!/usr/bin/env python3
import os, httpx, json, sys

BASE_URL = os.getenv("BASE_URL", "http://localhost:4000")
DEVICE_ID = os.getenv("DEVICE_ID", "sim_sensor_01")
FS = int(os.getenv("FS", "50"))
N = int(os.getenv("N", "4096"))
FREQ = float(os.getenv("FREQ", "8.5"))  # > 4 Hz to trigger
MAG = float(os.getenv("MAG", "30.0"))

payload = {
  "device_id": DEVICE_ID,
  "status": "atividade_detectada",
  "fs": FS, "n": N,
  "peaks": [ {"f": FREQ, "mag": MAG} ]
}

with httpx.Client(timeout=10.0) as cli:
  r = cli.post(f"{BASE_URL}/ingest/frequency", json=payload)
  print("POST /ingest/frequency", r.status_code, r.text)
