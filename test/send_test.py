import os, json, time, argparse, random, signal, sys
from dotenv import load_dotenv, find_dotenv
import paho.mqtt.client as mqtt
import threading

# 读取根目录 .env
load_dotenv(find_dotenv(".env"), override=True)

stop = False
def handle_sigint(sig, frame):
    global stop
    stop = True
signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)

def build_topic(device_id: str) -> str:
    t = os.getenv("MQTT_TOPIC", "devices/+/realtime")
    return t.replace("+", device_id, 1) if "+" in t else f"devices/{device_id}/realtime"

def rnd(a, b): return random.randint(a, b)

def gen_payload(customer_id: str | None, dealer_id: str | None) -> dict:
    # 随机生成 customer_id
    customer_id = f"C{random.randint(100, 999)}"
    soc  = rnd(0, 100)
    soh  = rnd(95, 100)
    pv   = rnd(0, 3500)
    load = rnd(200, 2500)
    grid = rnd(-1200, 1200)
    grid_q = rnd(0, 200)
    batt = pv - load - grid
    base_v = rnd(228000, 233000)  # mV
    return {
        "customer_id": customer_id,
        "dealer_id":   dealer_id or "D001",
        "soc": soc, "soh": soh,
        "pv": pv, "load": load, "grid": grid, "grid_q": grid_q, "batt": batt,
        "ac_v": base_v, "ac_f": 5000,
        "v_a": base_v, "v_b": base_v + rnd(-500, 500), "v_c": base_v + rnd(-500, 500),
        "i_a": rnd(500, 2500), "i_b": rnd(500, 2500), "i_c": rnd(500, 2500),
        "p_a": rnd(50, 800), "p_b": rnd(50, 800), "p_c": rnd(50, 800),
        "q_a": rnd(0, 50), "q_b": rnd(0, 50), "q_c": rnd(0, 50),
        "e_pv_today": rnd(1000, 80000), "e_load_today": rnd(1000, 90000),
        "e_charge_today": rnd(0, 50000), "e_discharge_today": rnd(0, 50000),
    }

def device_worker(device_id, args):
    host = "37.114.34.61"
    port = int(os.getenv("MQTT_PORT", "1883"))
    qos  = int(os.getenv("MQTT_QOS", "1"))
    user = os.getenv("MQTT_USERNAME") or None
    pwd  = os.getenv("MQTT_PASSWORD") or None
    topic = build_topic(device_id)

    client = mqtt.Client()
    if user:
        client.username_pw_set(user, pwd)

    def on_connect(c, u, f, rc, props=None):
        print(f"[{device_id}] [MQTT] connected rc=", rc)

    client.on_connect = on_connect
    client.connect(host, port, keepalive=30)
    client.loop_start()

    i = 0
    try:
        while not stop:
            i += 1
            payload = gen_payload(args.customer_id, args.dealer_id)
            data = json.dumps(payload, ensure_ascii=False)
            print(f"[{device_id}] [SEND {i}] {topic} -> {data}")
            r = client.publish(topic, data.encode("utf-8"), qos=qos, retain=args.retain)
            r.wait_for_publish()
            slept = 0.0
            while not stop and slept < args.interval:
                time.sleep(min(0.1, args.interval - slept))
                slept += 0.1
    finally:
        client.loop_stop()
        client.disconnect()
        print(f"[{device_id}] [MQTT] disconnected")

def main():
    parser = argparse.ArgumentParser(description="Continuously send test realtime data to EMQX (Ctrl+C to stop)")
    parser.add_argument("--interval", type=float, default=1.0, help="发送间隔秒（默认 1.0）")
    parser.add_argument("--retain", action="store_true", help="发布为保留消息")
    parser.add_argument("--customer_id", default=None)
    parser.add_argument("--dealer_id", default=None)
    args = parser.parse_args()

    threads = []
    for n in range(1, 11):
        device_id = f"DEV_{n:03d}"
        t = threading.Thread(target=device_worker, args=(device_id, args), daemon=True)
        t.start()
        threads.append(t)

    try:
        while not stop:
            time.sleep(0.5)
    finally:
        print("Stopping all device threads...")

if __name__ == "__main__":
    main()
