import os, json, time, signal
import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv, find_dotenv
from queue import Queue, Empty
from threading import Event, Thread

def log(*args, **kwargs):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), *args, flush=True, **kwargs)

load_dotenv(find_dotenv(".env"), override=True)

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME") or None
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD") or None
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "devices/+/realtime")
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))

PG_DSN = (
    f"host={os.getenv('PG_HOST','localhost')} "
    f"port={os.getenv('PG_PORT','5432')} "
    f"dbname={os.getenv('POSTGRES_DB','energy')} "
    f"user={os.getenv('POSTGRES_USER','admin')} "
    f"password={os.getenv('POSTGRES_PASSWORD','123456')}"
)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
FLUSH_MS = int(os.getenv("BATCH_FLUSH_MS", "1000"))
QUEUE_MAXSIZE = int(os.getenv("QUEUE_MAXSIZE", "10000"))

stop_event = Event()
q: Queue[dict] = Queue(maxsize=QUEUE_MAXSIZE)

FIELDS = [
    "dealer_id",
    "device_id",
    "soc", "soh",
    "pv", "load", "grid", "grid_q", "batt",
    "ac_v", "ac_f",
    "v_a", "v_b", "v_c",
    "i_a", "i_b", "i_c",
    "p_a", "p_b", "p_c",
    "q_a", "q_b", "q_c",
    "e_pv_today", "e_load_today", "e_charge_today", "e_discharge_today"
]

def parse_device_id(topic: str):
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0]=="devices" and parts[2]=="realtime":
        return parts[1]
    return None

def to_int(v, default=0):
    try:
        if v is None: return default
        return int(v)
    except Exception:
        return default

def normalize(sn: str, payload: dict) -> dict:
    d = {k: 0 for k in FIELDS}
    d["device_id"] = sn
    d["dealer_id"]   = payload.get("dealer_id") or ""
    d["soc"] = to_int(payload.get("soc"), 0)
    d["soh"] = to_int(payload.get("soh"), 100)
    for k in [
      "pv","load","grid","grid_q","batt",
      "ac_v","ac_f",
      "v_a","v_b","v_c",
      "i_a","i_b","i_c",
      "p_a","p_b","p_c",
      "q_a","q_b","q_c",
      "e_pv_today","e_load_today","e_charge_today","e_discharge_today",
    ]:
        d[k] = to_int(payload.get(k), 0)
    return d

UPSERT_SQL = """
INSERT INTO ess_realtime_data (
  dealer_id, device_id, updated_at,
  soc, soh,
  pv, load, grid, grid_q, batt,
  ac_v, ac_f,
  v_a, v_b, v_c,
  i_a, i_b, i_c,
  p_a, p_b, p_c,
  q_a, q_b, q_c,
  e_pv_today, e_load_today, e_charge_today, e_discharge_today
) VALUES (
  %(dealer_id)s, %(device_id)s, now(),
  %(soc)s, %(soh)s,
  %(pv)s, %(load)s, %(grid)s, %(grid_q)s, %(batt)s,
  %(ac_v)s, %(ac_f)s,
  %(v_a)s, %(v_b)s, %(v_c)s,
  %(i_a)s, %(i_b)s, %(i_c)s,
  %(p_a)s, %(p_b)s, %(p_c)s,
  %(q_a)s, %(q_b)s, %(q_c)s,
  %(e_pv_today)s, %(e_load_today)s, %(e_charge_today)s, %(e_discharge_today)s
)
ON CONFLICT (device_id) DO UPDATE SET
  dealer_id=EXCLUDED.dealer_id,
  updated_at=now(),
  soc=EXCLUDED.soc, soh=EXCLUDED.soh,
  pv=EXCLUDED.pv, load=EXCLUDED.load, grid=EXCLUDED.grid, grid_q=EXCLUDED.grid_q, batt=EXCLUDED.batt,
  ac_v=EXCLUDED.ac_v, ac_f=EXCLUDED.ac_f,
  v_a=EXCLUDED.v_a, v_b=EXCLUDED.v_b, v_c=EXCLUDED.v_c,
  i_a=EXCLUDED.i_a, i_b=EXCLUDED.i_b, i_c=EXCLUDED.i_c,
  p_a=EXCLUDED.p_a, p_b=EXCLUDED.p_b, p_c=EXCLUDED.p_c,
  q_a=EXCLUDED.q_a, q_b=EXCLUDED.q_b, q_c=EXCLUDED.q_c,
  e_pv_today=EXCLUDED.e_pv_today, e_load_today=EXCLUDED.e_load_today,
  e_charge_today=EXCLUDED.e_charge_today, e_discharge_today=EXCLUDED.e_discharge_today;
"""

def on_connect(client, userdata, flags, rc, props=None):
    if rc == 0:
        log(f"[MQTT] connected. subscribing {MQTT_TOPIC} qos={MQTT_QOS}")
        client.subscribe(MQTT_TOPIC, qos=MQTT_QOS)
    else:
        log(f"[MQTT] connect failed rc={rc}")

def on_message(client, userdata, msg):
    try:
        sn = parse_device_id(msg.topic)
        if not sn: return
        payload = json.loads(msg.payload.decode("utf-8"))
        rec = normalize(sn, payload)
        q.put(rec, block=False)
    except Exception as e:
        log("[on_message] error:", e)

def flusher():
    while not stop_event.is_set():
        try:
            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = False
            try:
                while not stop_event.is_set():
                    batch = []
                    deadline = time.time() + FLUSH_MS/1000.0
                    while len(batch) < BATCH_SIZE and time.time() < deadline:
                        try:
                            batch.append(q.get(timeout=0.05))
                        except Empty:
                            pass
                    if batch:
                        try:
                            with conn.cursor() as cur:
                                execute_batch(cur, UPSERT_SQL, batch, page_size=1000)
                            conn.commit()
                            log(f"[DB] upsert {len(batch)} rows")
                        except Exception as e:
                            log("[flusher] DB error:", e)
                            conn.rollback()
            finally:
                conn.close()
        except Exception as e:
            log("[flusher] fatal error, will retry in 5s:", e)
            time.sleep(5)

def main():
    t = Thread(target=flusher, daemon=True); t.start()
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        client = mqtt.Client()
    if MQTT_USERNAME: client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect; client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60); client.loop_start()

    def shutdown(sig, frm):
        log("shutting down..."); stop_event.set(); client.loop_stop(); client.disconnect()
    signal.signal(signal.SIGINT, shutdown); signal.signal(signal.SIGTERM, shutdown)

    try:
        while not stop_event.is_set():
            if not t.is_alive():
                log("[main] flusher thread died, shutting down...")
                stop_event.set()
            time.sleep(0.5)
    finally:
        stop_event.set()

if __name__ == "__main__":
    main()
