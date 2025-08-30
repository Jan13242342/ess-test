import time
import threading
import random
import paho.mqtt.client as mqtt

MQTT_HOST = "37.114.34.61"   # 改成你的MQTT服务器IP
MQTT_PORT = 1883
MQTT_QOS = 1

def build_topic(device_id) -> str:
    return f"devices/{device_id}/realtime"

def build_history_topic(device_id) -> str:
    return f"devices/{device_id}/history"

def rnd(a, b):
    return random.randint(a, b)

def gen_payload(device_id: int) -> dict:
    soc  = rnd(0, 100)
    soh  = rnd(95, 100)
    pv   = rnd(0, 3500)
    load = rnd(200, 2500)
    grid = rnd(-1200, 1200)
    grid_q = rnd(0, 200)
    batt = pv - load - grid
    base_v = rnd(228000, 233000)  # mV
    return {
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

def gen_history_payload(device_id: int, ts: str) -> dict:
    return {
        "device_id": device_id,
        "ts": ts,  # ISO格式时间字符串
        "charge_wh_total": rnd(10000, 50000),
        "discharge_wh_total": rnd(10000, 50000),
        "pv_wh_total": rnd(10000, 50000)
    }

def device_worker(device_id, interval=2, history_interval=10):
    while True:
        try:
            client = mqtt.Client()
            client.connect(MQTT_HOST, MQTT_PORT, 60)
            client.loop_start()
            topic = build_topic(device_id)
            history_topic = build_history_topic(device_id)
            last_history = time.time()
            while True:
                # 发送实时数据（不带dealer_id和user_id）
                payload = gen_payload(device_id)
                result = client.publish(topic, str(payload).replace("'", '"'), qos=MQTT_QOS)
                if result.rc != 0:
                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} publish失败: {result.rc}")
                    break
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} sent to {topic}")
                # 定时发送历史数据
                if time.time() - last_history >= history_interval:
                    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    his_payload = gen_history_payload(device_id, ts)
                    his_result = client.publish(history_topic, str(his_payload).replace("'", '"'), qos=MQTT_QOS)
                    if his_result.rc != 0:
                        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} history publish失败: {his_result.rc}")
                        break
                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} sent history to {history_topic}")
                    last_history = time.time()
                time.sleep(interval)
        except Exception as e:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} 发生异常: {e}")
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
            time.sleep(5)

if __name__ == "__main__":
    threads = []
    for n in range(1, 11):
        t = threading.Thread(target=device_worker, args=(n,), daemon=True)
        t.start()
        threads.append(t)
    while True:
        time.sleep(10)
