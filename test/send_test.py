import time
import threading
import random
import paho.mqtt.client as mqtt

# 直接写MQTT服务器IP和端口
MQTT_HOST = "37.114.34.61"   # <-- 这里改成你的远程MQTT服务器IP
MQTT_PORT = 1883
MQTT_TOPIC = "devices/+/realtime"
MQTT_QOS = 1

def build_topic(device_id) -> str:
    t = MQTT_TOPIC
    return t.replace("+", str(device_id), 1) if "+" in t else f"devices/{device_id}/realtime"

def rnd(a, b):
    return random.randint(a, b)

def gen_payload(dealer_id: int, device_id: int) -> dict:
    soc  = rnd(0, 100)
    soh  = rnd(95, 100)
    pv   = rnd(0, 3500)
    load = rnd(200, 2500)
    grid = rnd(-1200, 1200)
    grid_q = rnd(0, 200)
    batt = pv - load - grid
    base_v = rnd(228000, 233000)  # mV
    return {
        "dealer_id": dealer_id,
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

def device_worker(device_id, dealer_id, interval=2):
    while True:
        try:
            client = mqtt.Client()
            client.connect(MQTT_HOST, MQTT_PORT, 60)
            client.loop_start()  # 启动网络循环，保持心跳
            topic = build_topic(device_id)
            while True:
                payload = gen_payload(dealer_id, device_id)
                result = client.publish(topic, str(payload).replace("'", '"'), qos=MQTT_QOS)
                if result.rc != 0:
                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} publish失败: {result.rc}")
                    break  # 跳出内层循环，重新连接
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} dealer_id={dealer_id} sent to {topic}")
                time.sleep(interval)
        except Exception as e:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} 发生异常: {e}")
        finally:
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass
            time.sleep(5)  # 等待5秒后重连

if __name__ == "__main__":
    threads = []
    for n in range(1, 11):
        t = threading.Thread(target=device_worker, args=(n, n), daemon=True)
        t.start()
        threads.append(t)
    while True:
        time.sleep(10)
