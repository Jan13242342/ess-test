import time
import threading
import random
import paho.mqtt.client as mqtt
import json

MQTT_HOST = "37.114.34.61"   # 改成你的MQTT服务器IP
MQTT_PORT = 1883
MQTT_QOS = 0

def build_topic(device_id) -> str:
    return f"devices/{device_id}/realtime"

def build_history_topic(device_id) -> str:
    return f"devices/{device_id}/history"

def build_alarm_topic(device_id) -> str:
    return f"devices/{device_id}/alarm"

def build_para_topic(device_id) -> str:
    return f"devices/{device_id}/para"

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
        "pv_wh_total": rnd(10000, 50000),
        "grid_wh_total": rnd(-20000, 20000)  # 新增，允许正负
    }

def gen_alarm_payload(device_id: int, status: str) -> dict:
    code_level_map = {
        1001: "critical",
        2002: "major",
        3003: "minor",
        4004: "info"
    }
    alarm_types = ["system", "business", "communication", "hardware"]
    codes = list(code_level_map.keys())
    code = random.choice(codes)
    level = code_level_map[code]
    return {
        "alarm_type": random.choice(alarm_types),
        "code": code,
        "level": level,
        "extra": {"soc": rnd(0, 100), "note": "test alarm"},
        "status": status,
        "remark": f"测试报警 device {device_id}"
    }

def gen_para_payload(device_id: int) -> dict:
    # 所有参数都放在 para 字段，方便后端直接存 JSON
    para = {
        "discharge_power": rnd(100, 500),
        "charge_power": rnd(100, 500),
        "control_mode": random.choice(["test", "auto", "manual"]),
        "future_param": rnd(1, 100)  # 可扩展参数
    }
    return {
        "device_id": device_id,
        "para": para,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

def device_worker(device_id, interval=2, history_interval=300, alarm_interval=90, para_interval=90):
    last_alarm_status = "cleared"  # 初始为cleared，下次先发active
    last_alarm_payload = None
    while True:
        try:
            client = mqtt.Client()
            client.connect(MQTT_HOST, MQTT_PORT, 60)
            client.loop_start()
            topic = build_topic(device_id)
            history_topic = build_history_topic(device_id)
            alarm_topic = build_alarm_topic(device_id)
            para_topic = build_para_topic(device_id)
            last_history = time.time()
            last_alarm = time.time()
            last_para = time.time()
            while True:
                # 发送实时数据
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
                # 定时发送报警数据（active/cleared交替）
                if time.time() - last_alarm >= alarm_interval:
                    if last_alarm_status == "cleared":
                        # 发送active
                        alarm_payload_active = gen_alarm_payload(device_id, "active")
                        alarm_result_active = client.publish(alarm_topic, json.dumps(alarm_payload_active), qos=MQTT_QOS)
                        if alarm_result_active.rc != 0:
                            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} alarm(active) publish失败: {alarm_result_active.rc}")
                            break
                        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} sent alarm(active) to {alarm_topic}: {alarm_payload_active}")
                        last_alarm_payload = alarm_payload_active
                        last_alarm_status = "active"
                    else:
                        # 发送cleared
                        alarm_payload_cleared = last_alarm_payload.copy()
                        alarm_payload_cleared["status"] = "cleared"
                        alarm_payload_cleared["cleared_by"] = "device"
                        alarm_result_cleared = client.publish(alarm_topic, json.dumps(alarm_payload_cleared), qos=MQTT_QOS)
                        if alarm_result_cleared.rc != 0:
                            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} alarm(cleared) publish失败: {alarm_result_cleared.rc}")
                            break
                        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} sent alarm(cleared) to {alarm_topic}: {alarm_payload_cleared}")
                        last_alarm_status = "cleared"
                    last_alarm = time.time()
                # 定时发送参数数据（新版para结构）
                if time.time() - last_para >= para_interval:
                    para_payload = gen_para_payload(device_id)
                    para_result = client.publish(para_topic, json.dumps(para_payload), qos=MQTT_QOS)
                    if para_result.rc != 0:
                        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} para publish失败: {para_result.rc}")
                        break
                    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] device_id={device_id} sent para to {para_topic}: {para_payload}")
                    last_para = time.time()
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
