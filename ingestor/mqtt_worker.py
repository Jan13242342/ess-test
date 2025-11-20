import os, json, time
import paho.mqtt.client as mqtt

# 队列和 log 由 main.py 传入或全局导入   测试一下
from queue import Queue
from threading import Event

# 这些队列要和 main.py 保持一致
q: Queue = None
history_q: Queue = None
alarm_q: Queue = None
para_q: Queue = None
rpc_ack_q: Queue = None
archive_alarm_q: Queue = None
log = print

def set_queues(_q, _history_q, _alarm_q, _para_q, _rpc_ack_q, _archive_alarm_q, _log):
    global q, history_q, alarm_q, para_q, rpc_ack_q, archive_alarm_q, log
    q = _q
    history_q = _history_q
    alarm_q = _alarm_q
    para_q = _para_q
    rpc_ack_q = _rpc_ack_q
    archive_alarm_q = _archive_alarm_q
    log = _log

def _extract_sn_from_topic(topic: str):
    # handle $share/<group>/devices/<sn>/...
    parts = topic.split("/")
    offset = 0
    if parts and parts[0].startswith("$share"):
        # $share / <group> / devices / <sn> / ...
        if len(parts) >= 4:
            offset = 2
        else:
            return None
    if len(parts) >= offset + 3 and parts[offset] == "devices":
        return parts[offset + 1]
    # fallback: try common pattern devices/<sn>/...
    if len(parts) >= 2 and parts[0] == "devices":
        return parts[1]
    return None

def on_message(client, userdata, msg):
    log(f"[mqtt_worker] on_message topic={msg.topic} payload={msg.payload}")
    try:
        topic = msg.topic
        sn = _extract_sn_from_topic(topic)
        if not sn:
            return

        payload = json.loads(msg.payload.decode("utf-8") or "{}")
        # attach device identifiers for downstream consumers
        try:
            payload["device_id"] = int(sn)
        except Exception:
            payload["device_sn"] = sn

        if topic.endswith("/realtime"):
            # realtime: expect payload already contains realtime fields
            q.put(payload, block=False)

        elif topic.endswith("/history"):
            # ensure timestamp exists
            if not payload.get("ts"):
                payload["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            history_q.put(payload, block=False)

        elif topic.endswith("/alarm"):
            alarm_q.put(payload, block=False)

        elif topic.endswith("/para"):
            # ensure updated_at if not provided
            if not payload.get("updated_at"):
                payload["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            para_q.put(payload, block=False)

        elif topic.endswith("/rpc_ack"):
            # rpc_ack consumers expect device_sn or device_id and request_id/status/message
            if "device_sn" not in payload and "device_id" not in payload:
                payload["device_sn"] = sn
            rpc_ack_q.put(payload, block=False)
            log(f"[RPC_ACK] received from device {sn}: {payload.get('request_id') or payload.get('response_id')} -> {payload.get('status')}")
    except Exception as e:
        log("[on_message] error:", e)

def on_connect(client, userdata, flags, rc, properties=None):
    try:
        code = rc if isinstance(rc, int) else int(getattr(rc, "value", rc))
    except Exception:
        code = rc
    if code == 0:
        log("[mqtt] connected")
    else:
        log(f"[mqtt] connect failed rc={code}")

def setup_mqtt(client, topics, log_func=None):
    if log_func:
        global log
        log = log_func
    client.on_connect = on_connect
    client.on_message = on_message
    for topic, qos in topics:
        client.subscribe(topic, qos)
    log("[mqtt] subscribed:", ", ".join(t for t, _ in topics))
    log("[mqtt] subscribed:", ", ".join(t for t, _ in topics))