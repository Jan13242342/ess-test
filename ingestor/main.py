import os, json, time, signal
from queue import Queue
from threading import Event, Thread
import paho.mqtt.client as mqtt
from db_flushers import (
    flusher, history_flusher, alarm_flusher, para_flusher, rpc_ack_flusher, archive_alarm_worker
)
from dotenv import load_dotenv, find_dotenv
from utils import log, parse_device_id, to_int, normalize, normalize_history

# 加载 .env 文件中的环境变量（如果存在）
load_dotenv(find_dotenv(".env"), override=True)

# 读取MQTT和数据库相关配置
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

# 定义各类数据的队列和事件
stop_event = Event()
q = Queue(maxsize=QUEUE_MAXSIZE)              # 实时数据队列
history_q = Queue(maxsize=QUEUE_MAXSIZE)      # 历史能量数据队列
alarm_q = Queue(maxsize=QUEUE_MAXSIZE)        # 报警数据队列
para_q = Queue(maxsize=QUEUE_MAXSIZE)         # 参数数据队列
rpc_ack_q = Queue(maxsize=QUEUE_MAXSIZE)      # RPC应答队列
archive_alarm_q = Queue(maxsize=QUEUE_MAXSIZE)# 报警归档队列

# 实时数据字段定义
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

# 实时数据 UPSERT SQL
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

# 历史能量数据相关配置
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
    """
    归一化历史能量数据，确保字段齐全。
    """
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

# 报警数据相关配置
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
ON CONFLICT (device_id, alarm_type, code)
DO UPDATE SET
  last_triggered_at = EXCLUDED.last_triggered_at,
  repeat_count = alarms.repeat_count + 1,
  level = EXCLUDED.level,
  extra = EXCLUDED.extra,
  remark = EXCLUDED.remark;
"""

# 参数数据相关配置
PARA_TOPIC = os.getenv("MQTT_PARA_TOPIC", "$share/ess-ingestor/devices/+/para")
para_q: Queue[dict] = Queue(maxsize=QUEUE_MAXSIZE)

PARA_UPSERT_SQL = """
INSERT INTO device_para (device_id, para, updated_at)
VALUES (%(device_id)s, %(para)s::jsonb, %(updated_at)s)
ON CONFLICT (device_id) DO UPDATE SET
  para = EXCLUDED.para,
  updated_at = EXCLUDED.updated_at;
"""

# RPC应答相关配置
RPC_ACK_TOPIC = os.getenv("MQTT_RPC_ACK_TOPIC", "$share/ess-ingestor/devices/+/rpc_ack")
rpc_ack_q: Queue[dict] = Queue(maxsize=QUEUE_MAXSIZE)

RPC_ACK_UPDATE_SQL = """
UPDATE device_rpc_change_log 
SET status=%(status)s,
    confirmed_at=now(),
    message = COALESCE(%(message)s, message)
WHERE request_id=%(request_id)s 
  AND device_id=(SELECT id FROM devices WHERE device_sn=%(device_sn)s)
  AND status = 'pending'
"""

def on_message(client, userdata, msg):
    """
    MQTT消息回调，根据不同topic类型解析数据并放入对应队列。
    """
    try:
        if msg.topic.endswith("/realtime"):
            sn = parse_device_id(msg.topic, "realtime")
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))
            rec = normalize(sn, payload, FIELDS)
            q.put(rec, block=False)

        elif msg.topic.endswith("/history"):
            sn = parse_device_id(msg.topic, "history")
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))
            rec = normalize_history(sn, payload)
            history_q.put(rec, block=False)

        elif msg.topic.endswith("/alarm"):
            sn = parse_device_id(msg.topic, "alarm")
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))
            now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            alarm = {
                "device_id": int(sn),
                "alarm_type": payload.get("alarm_type", "unknown"),
                "code": int(payload.get("code", 0)),
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
            should_archive = False
            if alarm["level"] == "critical":
                if alarm["status"] == "cleared" and alarm["confirmed_at"]:
                    should_archive = True
            else:
                if alarm["status"] == "cleared":
                    should_archive = True
            (archive_alarm_q if should_archive else alarm_q).put(alarm, block=False)

        elif msg.topic.endswith("/para"):
            sn = parse_device_id(msg.topic, "para")
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))
            para = {
                "device_id": int(sn),
                "para": payload,
                "updated_at": payload.get("updated_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
            }
            para_q.put(para, block=False)

        elif msg.topic.endswith("/rpc_ack"):
            sn = parse_device_id(msg.topic, "rpc_ack")
            if not sn: return
            payload = json.loads(msg.payload.decode("utf-8"))

            response_id = payload.get("response_id")
            status = payload.get("status", "error")
            if not response_id:
                log(f"[RPC_ACK] missing response_id from device {sn}")
                return
            if status not in ["success", "failed", "error", "timeout"]:
                status = "error"

            msg_text = payload.get("message")
            if isinstance(msg_text, (dict, list)):
                msg_text = json.dumps(msg_text, ensure_ascii=False)

            rpc_ack = {
                "device_sn": sn,
                "request_id": response_id,
                "status": status,
                "message": msg_text,
            }
            rpc_ack_q.put(rpc_ack, block=False)
            log(f"[RPC_ACK] received from device {sn}: {response_id} -> {status}")

    except Exception as e:
        log("[on_message] error:", e)

def ensure_devices_exist(cur, batch):
    """
    批量 upsert 设备，避免外键约束报错
    """
    device_ids = set(row["device_id"] for row in batch)
    if not device_ids:
        return
    from psycopg2.extras import execute_batch
    execute_batch(
        cur,
        "INSERT INTO devices (id, device_sn, created_at) VALUES (%s, %s, now()) ON CONFLICT (id) DO NOTHING;",
        [(did, f"SN{did:04d}") for did in device_ids]
    )

def on_connect(client, userdata, flags, rc, properties=None):
    """
    MQTT连接回调，连接成功后订阅所有相关主题。
    """
    try:
        code = rc if isinstance(rc, int) else int(getattr(rc, "value", rc))
    except Exception:
        code = rc
    if code == 0:
        log("[mqtt] connected")
        client.subscribe(MQTT_TOPIC, MQTT_QOS)
        client.subscribe(HISTORY_TOPIC, MQTT_QOS)
        client.subscribe(ALARM_TOPIC, MQTT_QOS)
        client.subscribe(PARA_TOPIC, MQTT_QOS)
        client.subscribe(RPC_ACK_TOPIC, MQTT_QOS)
        log("[mqtt] subscribed:",
            MQTT_TOPIC,
            HISTORY_TOPIC,
            ALARM_TOPIC,
            PARA_TOPIC,
            RPC_ACK_TOPIC,
        )
    else:
        log(f"[mqtt] connect failed rc={code}")

def main():
    """
    主入口：启动各类数据库写入线程，初始化MQTT客户端并进入主循环。
    """
    # 启动各类数据写入线程
    t = Thread(target=flusher, args=(stop_event, q, PG_DSN, UPSERT_SQL, BATCH_SIZE, FLUSH_MS), daemon=True)
    ht = Thread(target=history_flusher, args=(stop_event, history_q, PG_DSN, HISTORY_UPSERT_SQL, BATCH_SIZE, FLUSH_MS), daemon=True)
    at = Thread(target=alarm_flusher, args=(stop_event, alarm_q, PG_DSN, ALARM_UPSERT_SQL, BATCH_SIZE, FLUSH_MS), daemon=True)
    pt = Thread(target=para_flusher, args=(stop_event, para_q, PG_DSN, PARA_UPSERT_SQL, BATCH_SIZE, FLUSH_MS), daemon=True)
    rt = Thread(target=rpc_ack_flusher, args=(stop_event, rpc_ack_q, PG_DSN, RPC_ACK_UPDATE_SQL, BATCH_SIZE, FLUSH_MS), daemon=True)
    arch = Thread(target=archive_alarm_worker, args=(stop_event, archive_alarm_q, PG_DSN), daemon=True)
    t.start(); ht.start(); at.start(); pt.start(); rt.start(); arch.start()

    # 初始化并启动MQTT客户端
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        client = mqtt.Client()
    if MQTT_USERNAME: client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect; client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60); client.loop_start()

    # 信号处理，优雅关闭
    def shutdown(sig, frm):
        log("shutting down..."); stop_event.set(); client.loop_stop(); client.disconnect()
    signal.signal(signal.SIGINT, shutdown); signal.signal(signal.SIGTERM, shutdown)

    # 主循环，监控线程存活
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