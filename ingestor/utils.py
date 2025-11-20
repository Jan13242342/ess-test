import time

def log(*args, **kwargs):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), *args, flush=True, **kwargs)

def parse_device_id(topic: str, suffix: str):
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0] == "devices" and parts[2] == suffix:
        return parts[1]
    return None

def to_int(v, default=0):
    try:
        if v is None: return default
        return int(v)
    except Exception:
        return default

def normalize(sn: str, payload: dict, fields: list) -> dict:
    d = {k: 0 for k in fields}
    d["device_id"] = int(sn)
    d["soc"] = to_int(payload.get("soc"), 0)
    d["soh"] = to_int(payload.get("soh"), 100)
    for k in fields:
        if k not in ["device_id", "soc", "soh"]:
            d[k] = to_int(payload.get(k), 0)
    return d

def normalize_history(sn: str, payload: dict):
    ts = payload.get("ts")
    if not ts:
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return {
        "device_id": int(sn),
        "ts": ts,
        "charge_wh_total": int(payload.get("charge_wh_total", 0)),
        "discharge_wh_total": int(payload.get("discharge_wh_total", 0)),
        "pv_wh_total": int(payload.get("pv_wh_total", 0)),
        "grid_wh_total": int(payload.get("grid_wh_total", 0)),
        "load_wh_total": int(payload.get("load_wh_total", 0))
    }