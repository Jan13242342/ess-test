import os, json, time, signal
import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv, find_dotenv
from queue import Queue, Empty
from threading import Event, Thread
import dateutil.parser

def log(*args, **kwargs):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), *args, flush=True, **kwargs)

load_dotenv(find_dotenv(".env"), override=True)

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME") or None
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD") or None
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "$share/ess-ingestor/devices/+/realtime")
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

def parse_history_device_id(topic: str):
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0]=="devices" and parts[2]=="history":
        return parts[1]
    return None

def parse_alarm_device_id(topic: str):
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0]=="devices" and parts[2]=="alarm":
        return parts[1]
    return None

def parse_para_device_id(topic: str):
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0]=="devices" and parts[2]=="para":
        return parts[1]
    return None

def parse_rpc_ack_device_id(topic: str):
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0] == "devices" and parts[2] == "rpc_ack":
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
    d["device_id"] = int(sn)
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
  device_id, updated_at,
  soc, soh,
  pv, load, grid, grid_q, batt,
  ac_v, ac_f,
  v_a, v_b, v_c,
  i_a, i_b, i_c,
  p_a, p_b, p_c,
  q_a, q_b, q_c,
  e_pv_today, e_load_today, e_charge_today, e_discharge_today
) VALUES (
  %(device_id)s, now(),
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

# 新增历史能量数据的共享订阅和处理
HISTORY_TOPIC = os.getenv("MQTT_HISTORY_TOPIC", "$share/ess-ingestor/devices/+/history")
history_q: Queue[dict] = Queue(maxsize=QUEUE_MAXSIZE)

HISTORY_UPSERT_SQL = """
INSERT INTO history_energy (
  device_id, ts, charge_wh_total, discharge_wh_total, pv_wh_total, grid_wh_total, load_wh_total
) VALUES (
  %(device_id)s, %(ts)s, %(charge_wh_total)s, %(discharge_wh_total)s, %(pv_wh_total)s, %(grid_wh_total)s, %(load_wh_total)s
)
ON CONFLICT (device_id, ts) DO UPDATE SET
  charge_wh_total=EXCLUDED.charge_wh_total,
  discharge_wh_total=EXCLUDED.discharge_wh_total,
  pv_wh_total=EXCLUDED.pv_wh_total,
  grid_wh_total=EXCLUDED.grid_wh_total,
  load_wh_total=EXCLUDED.load_wh_total;
"""

def normalize_history(sn: str, payload: dict) -> dict:
    return {
        "device_id": int(sn),
        "ts": payload.get("ts"),
        "charge_wh_total": int(payload.get("charge_wh_total", 0)),
        "discharge_wh_total": int(payload.get("discharge_wh_total", 0)),
        "pv_wh_total": int(payload.get("pv_wh_total", 0)),
        "grid_wh_total": int(payload.get("grid_wh_total", 0)),  # 新增
        "load_wh_total": int(payload.get("load_wh_total", 0))  # 新增
    }

# 新增报警topic和队列
ALARM_TOPIC = os.getenv("MQTT_ALARM_TOPIC", "$share/ess-ingestor/devices/+/alarm")
alarm_q: Queue[dict] = Queue(maxsize=QUEUE_MAXSIZE)

ALARM_UPSERT_SQL = """
INSERT INTO alarms (
  device_id, alarm_type, code, level, extra, status,
  first_triggered_at, last_triggered_at, repeat_count, remark,
  confirmed_at, confirmed_by, cleared_at, cleared_by
) VALUES (
  %(device_id)s, %(alarm_type)s, %(code)s, %(level)s, %(extra)s, %(status)s,
  %(first_triggered_at)s, %(last_triggered_at)s, %(repeat_count)s, %(remark)s,
  %(confirmed_at)s, %(confirmed_by)s, %(cleared_at)s, %(cleared_by)s
)
ON CONFLICT (device_id, alarm_type, code, status)
DO UPDATE SET
  last_triggered_at = EXCLUDED.last_triggered_at,
  repeat_count = alarms.repeat_count + 1,
  level = EXCLUDED.level,
  extra = EXCLUDED.extra,
  remark = EXCLUDED.remark;
"""

# 新增 para topic 和队列
PARA_TOPIC = os.getenv("MQTT_PARA_TOPIC", "$share/ess-ingestor/devices/+/para")
para_q: Queue[dict] = Queue(maxsize=QUEUE_MAXSIZE)

# 新版参数 upsert SQL
PARA_UPSERT_SQL = """
INSERT INTO device_para (device_id, para, updated_at)
VALUES (%(device_id)s, %(para)s::jsonb, %(updated_at)s)
ON CONFLICT (device_id) DO UPDATE SET
  para = EXCLUDED.para,
  updated_at = EXCLUDED.updated_at;
"""

# 新增 RPC 确认 topic 和队列
RPC_ACK_TOPIC = os.getenv("MQTT_RPC_ACK_TOPIC", "$share/ess-ingestor/devices/+/rpc_ack")
rpc_ack_q: Queue[dict] = Queue(maxsize=QUEUE_MAXSIZE)

# 修改 RPC 确认更新 SQL - 移除超时检测，只更新 pending 状态
RPC_ACK_UPDATE_SQL = """
UPDATE device_rpc_change_log 
SET status=%(status)s, confirmed_at=now()
WHERE request_id=%(request_id)s 
AND device_id=(SELECT id FROM devices WHERE device_sn=%(device_sn)s)
AND status = 'pending'
"""

def on_connect(client, userdata, flags, rc, props=None):
    if rc == 0:
        log(f"[MQTT] connected. subscribing {MQTT_TOPIC} qos=0")
        client.subscribe(MQTT_TOPIC, qos=0)
        log(f"[MQTT] subscribing {HISTORY_TOPIC} qos=0")
        client.subscribe(HISTORY_TOPIC, qos=0)
        log(f"[MQTT] subscribing {ALARM_TOPIC} qos=1")
        client.subscribe(ALARM_TOPIC, qos=1)
        log(f"[MQTT] subscribing {PARA_TOPIC} qos=1")
        client.subscribe(PARA_TOPIC, qos=0)
        log(f"[MQTT] subscribing {RPC_ACK_TOPIC} qos=1")
        client.subscribe(RPC_ACK_TOPIC, qos=1)
    else:
        log(f"[MQTT] connect failed rc={rc}")

def on_message(client, userdata, msg):
    try:
        if msg.topic.endswith("/realtime"):
            sn = parse_device_id(msg.topic)
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))
            rec = normalize(sn, payload)
            q.put(rec, block=False)
        elif msg.topic.endswith("/history"):
            sn = parse_history_device_id(msg.topic)
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))
            rec = normalize_history(sn, payload)
            history_q.put(rec, block=False)
        elif msg.topic.endswith("/alarm"):
            sn = parse_alarm_device_id(msg.topic)
            if not sn:
                return
            payload = json.loads(msg.payload.decode("utf-8"))
            now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            alarm = {
                "device_id": int(sn),
                "alarm_type": payload.get("alarm_type", "unknown"),
                "code": int(payload.get("code", 0)),  # 强制转int，确保类型一致
                "level": payload.get("level", "info"),
                "extra": json.dumps(payload.get("extra", {})),
                "status": payload.get("status", "active"),
                "first_triggered_at": now,
                "last_triggered_at": now,
                "repeat_count": 1,
                "remark": payload.get("remark", None),
                "confirmed_at": payload.get("confirmed_at") or (now if payload.get("status") == "confirmed" or payload.get("confirmed_by") else None),
                "confirmed_by": payload.get("confirmed_by", None),
                "cleared_at": payload.get("cleared_at") or (now if payload.get("status") == "cleared" else None),
                "cleared_by": payload.get("cleared_by", None),
            }
            # 归档逻辑调整
            should_archive = False
            if alarm["level"] == "critical":
                # critical 级别，只有确认且清除才归档
                if alarm["status"] == "cleared" and alarm["confirmed_at"]:
                    should_archive = True
            else:
                # 其它级别，只要清除就归档
                if alarm["status"] == "cleared":
                    should_archive = True

            if should_archive:
                archive_alarm_q.put(alarm, block=False)
            else:
                alarm_q.put(alarm, block=False)
        elif msg.topic.endswith("/para"):
            sn = parse_para_device_id(msg.topic)
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))
            para = {
                "device_id": int(sn),
                "para": payload,  # 直接存整个参数为 JSON
                "updated_at": payload.get("updated_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
            }
            para_q.put(para, block=False)
        elif msg.topic.endswith("/rpc_ack"):
            sn = parse_rpc_ack_device_id(msg.topic)
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))
            
            # 验证必要字段 - 使用 response_id
            response_id = payload.get("response_id")
            status = payload.get("status", "error")
            if not response_id:
                log(f"[RPC_ACK] missing response_id from device {sn}")
                return
            
            # 验证状态值
            valid_statuses = ["success", "failed", "error", "timeout"]
            if status not in valid_statuses:
                status = "error"
            
            rpc_ack = {
                "device_sn": sn,
                "request_id": response_id,  # 数据库字段仍叫 request_id，但值来自 response_id
                "status": status
            }
            rpc_ack_q.put(rpc_ack, block=False)
            log(f"[RPC_ACK] received from device {sn}: {response_id} -> {status}")
            
    except Exception as e:
        log("[on_message] error:", e)

def ensure_devices_exist(cur, batch):
    # 批量 upsert 设备，避免外键约束报错
    device_ids = set(row["device_id"] for row in batch)
    if not device_ids:
        return
    execute_batch(
        cur,
        "INSERT INTO devices (id, device_sn, created_at) VALUES (%s, %s, now()) ON CONFLICT (id) DO NOTHING;",
        [(did, f"SN{did:04d}") for did in device_ids]
    )

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
                                ensure_devices_exist(cur, batch)  # 先批量 upsert 设备
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

def ensure_devices_exist_history(cur, batch):
    # 批量 upsert 设备，避免外键约束报错
    device_ids = set(row["device_id"] for row in batch)
    if not device_ids:
        return
    execute_batch(
        cur,
        "INSERT INTO devices (id, device_sn, created_at) VALUES (%s, %s, now()) ON CONFLICT (id) DO NOTHING;",
        [(did, f"SN{did:04d}") for did in device_ids]
    )

def history_flusher():
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
                            batch.append(history_q.get(timeout=0.05))
                        except Empty:
                            pass
                    if batch:
                        try:
                            with conn.cursor() as cur:
                                ensure_devices_exist_history(cur, batch)  # 先批量 upsert 设备
                                execute_batch(cur, HISTORY_UPSERT_SQL, batch, page_size=1000)
                            conn.commit()
                            log(f"[DB] upsert history {len(batch)} rows")
                        except Exception as e:
                            log("[history_flusher] DB error:", e)
                            conn.rollback()
            finally:
                conn.close()
        except Exception as e:
            log("[history_flusher] fatal error, will retry in 5s:", e)
            time.sleep(5)

def alarm_flusher():
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
                            batch.append(alarm_q.get(timeout=0.05))
                        except Empty:
                            pass
                    if batch:
                        try:
                            with conn.cursor() as cur:
                                execute_batch(cur, ALARM_UPSERT_SQL, batch, page_size=1000)
                            conn.commit()
                            log(f"[DB] upsert alarm {len(batch)} rows")
                        except Exception as e:
                            log("[alarm_flusher] DB error:", e)
                            conn.rollback()
            finally:
                conn.close()
        except Exception as e:
            log("[alarm_flusher] fatal error, will retry in 5s:", e)
            time.sleep(5)

def para_flusher():
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
                            batch.append(para_q.get(timeout=0.05))
                        except Empty:
                            pass
                    if batch:
                        try:
                            with conn.cursor() as cur:
                                # 组装参数
                                upsert_data = [
                                    {
                                        "device_id": rec["device_id"],
                                        "para": json.dumps(rec["para"]),
                                        "updated_at": rec["updated_at"]
                                    }
                                    for rec in batch
                                ]
                                psycopg2.extras.execute_batch(cur, PARA_UPSERT_SQL, upsert_data, page_size=1000)
                            conn.commit()
                            log(f"[DB] upsert {len(batch)} para rows")
                        except Exception as e:
                            log("[para_flusher] DB error:", e)
                            conn.rollback()
            finally:
                conn.close()
        except Exception as e:
            log("[para_flusher] fatal error, will retry in 5s:", e)
            time.sleep(5)

def rpc_ack_flusher():
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
                            batch.append(rpc_ack_q.get(timeout=0.05))
                        except Empty:
                            pass
                    if batch:
                        try:
                            with conn.cursor() as cur:
                                execute_batch(cur, RPC_ACK_UPDATE_SQL, batch, page_size=1000)
                            conn.commit()
                            log(f"[DB] update rpc_ack {len(batch)} rows")
                        except Exception as e:
                            log("[rpc_ack_flusher] DB error:", e)
                            conn.rollback()
            finally:
                conn.close()
        except Exception as e:
            log("[rpc_ack_flusher] fatal error, will retry in 5s:", e)
            time.sleep(5)

def parse_dt(val):
    if isinstance(val, str):
        return dateutil.parser.parse(val)
    return val

# 新增归档队列
archive_alarm_q: Queue[dict] = Queue(maxsize=QUEUE_MAXSIZE)

def archive_alarm_worker():
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            while not stop_event.is_set():
                try:
                    alarm = archive_alarm_q.get(timeout=0.1)
                except Empty:
                    continue
                try:
                    cur.execute(
                        "SELECT * FROM alarms WHERE device_id=%s AND alarm_type=%s AND code=%s AND status!='cleared' ORDER BY last_triggered_at DESC LIMIT 1",
                        (alarm["device_id"], alarm["alarm_type"], alarm["code"])
                    )
                    row = cur.fetchone()
                    if row:
                        cleared_at = parse_dt(alarm["cleared_at"])
                        first_triggered_at = parse_dt(row["first_triggered_at"])
                        duration = (cleared_at - first_triggered_at) if cleared_at and first_triggered_at else None
                        cur.execute(
                            """
                            INSERT INTO alarm_history (
                                device_id, alarm_type, code, level, extra, status,
                                first_triggered_at, last_triggered_at, repeat_count, remark,
                                confirmed_at, confirmed_by, cleared_at, cleared_by, archived_at, duration
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s, now(), %s
                            )
                            """,
                            (
                                row["device_id"], row["alarm_type"], row["code"], row["level"],
                                json.dumps(row["extra"]) if isinstance(row["extra"], dict) else row["extra"],
                                "cleared",
                                row["first_triggered_at"], row["last_triggered_at"], row["repeat_count"], row["remark"],
                                row["confirmed_at"], row["confirmed_by"], alarm["cleared_at"], alarm["cleared_by"],
                                duration
                            )
                        )
                        cur.execute(
                            "DELETE FROM alarms WHERE id=%s", (row["id"],)
                        )
                        conn.commit()
                except Exception as e:
                    log("[alarm archive] error:", e)
                    conn.rollback()
    except Exception as e:
        log("[archive_alarm_worker] fatal error:", e)
    finally:
        try:
            conn.close()
        except:
            pass

# 修改 on_message 归档部分为投递到队列
def on_message(client, userdata, msg):
    try:
        if msg.topic.endswith("/realtime"):
            sn = parse_device_id(msg.topic)
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))
            rec = normalize(sn, payload)
            q.put(rec, block=False)
        elif msg.topic.endswith("/history"):
            sn = parse_history_device_id(msg.topic)
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))
            rec = normalize_history(sn, payload)
            history_q.put(rec, block=False)
        elif msg.topic.endswith("/alarm"):
            sn = parse_alarm_device_id(msg.topic)
            if not sn:
                return
            payload = json.loads(msg.payload.decode("utf-8"))
            now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            alarm = {
                "device_id": int(sn),
                "alarm_type": payload.get("alarm_type", "unknown"),
                "code": int(payload.get("code", 0)),  # 强制转int，确保类型一致
                "level": payload.get("level", "info"),
                "extra": json.dumps(payload.get("extra", {})),
                "status": payload.get("status", "active"),
                "first_triggered_at": now,
                "last_triggered_at": now,
                "repeat_count": 1,
                "remark": payload.get("remark", None),
                "confirmed_at": payload.get("confirmed_at") or (now if payload.get("status") == "confirmed" or payload.get("confirmed_by") else None),
                "confirmed_by": payload.get("confirmed_by", None),
                "cleared_at": payload.get("cleared_at") or (now if payload.get("status") == "cleared" else None),
                "cleared_by": payload.get("cleared_by", None),
            }
            # 归档逻辑调整
            should_archive = False
            if alarm["level"] == "critical":
                # critical 级别，只有确认且清除才归档
                if alarm["status"] == "cleared" and alarm["confirmed_at"]:
                    should_archive = True
            else:
                # 其它级别，只要清除就归档
                if alarm["status"] == "cleared":
                    should_archive = True

            if should_archive:
                archive_alarm_q.put(alarm, block=False)
            else:
                alarm_q.put(alarm, block=False)
        elif msg.topic.endswith("/para"):
            sn = parse_para_device_id(msg.topic)
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))
            para = {
                "device_id": int(sn),
                "para": payload,  # 直接存整个参数为 JSON
                "updated_at": payload.get("updated_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
            }
            para_q.put(para, block=False)
        elif msg.topic.endswith("/rpc_ack"):
            sn = parse_rpc_ack_device_id(msg.topic)
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))
            
            # 验证必要字段 - 使用 response_id
            response_id = payload.get("response_id")
            status = payload.get("status", "error")
            if not response_id:
                log(f"[RPC_ACK] missing response_id from device {sn}")
                return
            
            # 验证状态值
            valid_statuses = ["success", "failed", "error", "timeout"]
            if status not in valid_statuses:
                status = "error"
            
            rpc_ack = {
                "device_sn": sn,
                "request_id": response_id,  # 数据库字段仍叫 request_id，但值来自 response_id
                "status": status
            }
            rpc_ack_q.put(rpc_ack, block=False)
            log(f"[RPC_ACK] received from device {sn}: {response_id} -> {status}")
            
    except Exception as e:
        log("[on_message] error:", e)

def ensure_devices_exist(cur, batch):
    # 批量 upsert 设备，避免外键约束报错
    device_ids = set(row["device_id"] for row in batch)
    if not device_ids:
        return
    execute_batch(
        cur,
        "INSERT INTO devices (id, device_sn, created_at) VALUES (%s, %s, now()) ON CONFLICT (id) DO NOTHING;",
        [(did, f"SN{did:04d}") for did in device_ids]
    )

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
                                ensure_devices_exist(cur, batch)  # 先批量 upsert 设备
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

def ensure_devices_exist_history(cur, batch):
    # 批量 upsert 设备，避免外键约束报错
    device_ids = set(row["device_id"] for row in batch)
    if not device_ids:
        return
    execute_batch(
        cur,
        "INSERT INTO devices (id, device_sn, created_at) VALUES (%s, %s, now()) ON CONFLICT (id) DO NOTHING;",
        [(did, f"SN{did:04d}") for did in device_ids]
    )

def history_flusher():
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
                            batch.append(history_q.get(timeout=0.05))
                        except Empty:
                            pass
                    if batch:
                        try:
                            with conn.cursor() as cur:
                                ensure_devices_exist_history(cur, batch)  # 先批量 upsert 设备
                                execute_batch(cur, HISTORY_UPSERT_SQL, batch, page_size=1000)
                            conn.commit()
                            log(f"[DB] upsert history {len(batch)} rows")
                        except Exception as e:
                            log("[history_flusher] DB error:", e)
                            conn.rollback()
            finally:
                conn.close()
        except Exception as e:
            log("[history_flusher] fatal error, will retry in 5s:", e)
            time.sleep(5)

def alarm_flusher():
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
                            batch.append(alarm_q.get(timeout=0.05))
                        except Empty:
                            pass
                    if batch:
                        try:
                            with conn.cursor() as cur:
                                execute_batch(cur, ALARM_UPSERT_SQL, batch, page_size=1000)
                            conn.commit()
                            log(f"[DB] upsert alarm {len(batch)} rows")
                        except Exception as e:
                            log("[alarm_flusher] DB error:", e)
                            conn.rollback()
            finally:
                conn.close()
        except Exception as e:
            log("[alarm_flusher] fatal error, will retry in 5s:", e)
            time.sleep(5)

def para_flusher():
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
                            batch.append(para_q.get(timeout=0.05))
                        except Empty:
                            pass
                    if batch:
                        try:
                            with conn.cursor() as cur:
                                # 组装参数
                                upsert_data = [
                                    {
                                        "device_id": rec["device_id"],
                                        "para": json.dumps(rec["para"]),
                                        "updated_at": rec["updated_at"]
                                    }
                                    for rec in batch
                                ]
                                psycopg2.extras.execute_batch(cur, PARA_UPSERT_SQL, upsert_data, page_size=1000)
                            conn.commit()
                            log(f"[DB] upsert {len(batch)} para rows")
                        except Exception as e:
                            log("[para_flusher] DB error:", e)
                            conn.rollback()
            finally:
                conn.close()
        except Exception as e:
            log("[para_flusher] fatal error, will retry in 5s:", e)
            time.sleep(5)

def rpc_ack_flusher():
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
                            batch.append(rpc_ack_q.get(timeout=0.05))
                        except Empty:
                            pass
                    if batch:
                        try:
                            with conn.cursor() as cur:
                                execute_batch(cur, RPC_ACK_UPDATE_SQL, batch, page_size=1000)
                            conn.commit()
                            log(f"[DB] update rpc_ack {len(batch)} rows")
                        except Exception as e:
                            log("[rpc_ack_flusher] DB error:", e)
                            conn.rollback()
            finally:
                conn.close()
        except Exception as e:
            log("[rpc_ack_flusher] fatal error, will retry in 5s:", e)
            time.sleep(5)

def parse_dt(val):
    if isinstance(val, str):
        return dateutil.parser.parse(val)
    return val

# 新增归档队列
archive_alarm_q: Queue[dict] = Queue(maxsize=QUEUE_MAXSIZE)

def archive_alarm_worker():
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            while not stop_event.is_set():
                try:
                    alarm = archive_alarm_q.get(timeout=0.1)
                except Empty:
                    continue
                try:
                    cur.execute(
                        "SELECT * FROM alarms WHERE device_id=%s AND alarm_type=%s AND code=%s AND status!='cleared' ORDER BY last_triggered_at DESC LIMIT 1",
                        (alarm["device_id"], alarm["alarm_type"], alarm["code"])
                    )
                    row = cur.fetchone()
                    if row:
                        cleared_at = parse_dt(alarm["cleared_at"])
                        first_triggered_at = parse_dt(row["first_triggered_at"])
                        duration = (cleared_at - first_triggered_at) if cleared_at and first_triggered_at else None
                        cur.execute(
                            """
                            INSERT INTO alarm_history (
                                device_id, alarm_type, code, level, extra, status,
                                first_triggered_at, last_triggered_at, repeat_count, remark,
                                confirmed_at, confirmed_by, cleared_at, cleared_by, archived_at, duration
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s, now(), %s
                            )
                            """,
                            (
                                row["device_id"], row["alarm_type"], row["code"], row["level"],
                                json.dumps(row["extra"]) if isinstance(row["extra"], dict) else row["extra"],
                                "cleared",
                                row["first_triggered_at"], row["last_triggered_at"], row["repeat_count"], row["remark"],
                                row["confirmed_at"], row["confirmed_by"], alarm["cleared_at"], alarm["cleared_by"],
                                duration
                            )
                        )
                        cur.execute(
                            "DELETE FROM alarms WHERE id=%s", (row["id"],)
                        )
                        conn.commit()
                except Exception as e:
                    log("[alarm archive] error:", e)
                    conn.rollback()
    except Exception as e:
        log("[archive_alarm_worker] fatal error:", e)
    finally:
        try:
            conn.close()
        except:
            pass

def main():
    t = Thread(target=flusher, daemon=True); t.start()
    ht = Thread(target=history_flusher, daemon=True); ht.start()
    at = Thread(target=alarm_flusher, daemon=True); at.start()
    pt = Thread(target=para_flusher, daemon=True); pt.start()
    rt = Thread(target=rpc_ack_flusher, daemon=True); rt.start()
    arch = Thread(target=archive_alarm_worker, daemon=True); arch.start()  # 新增归档线程
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
            if not t.is_alive() or not ht.is_alive() or not at.is_alive() or not pt.is_alive() or not rt.is_alive():
                log("[main] flusher thread died, shutting down...")
                stop_event.set()
            time.sleep(0.5)
    finally:
        stop_event.set()

if __name__ == "__main__":
    main()
