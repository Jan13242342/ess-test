import time
import json
import psycopg2
from psycopg2.extras import execute_batch
from queue import Empty
from threading import Event
import dateutil.parser

def log(*args, **kwargs):
    print(time.strftime("[%Y-%m-%d %H:%M:%S]"), *args, flush=True, **kwargs)

def ensure_devices_exist(cur, batch):
    device_ids = set(row["device_id"] for row in batch)
    if not device_ids:
        return
    execute_batch(
        cur,
        "INSERT INTO devices (id, device_sn, created_at) VALUES (%s, %s, now()) ON CONFLICT (id) DO NOTHING;",
        [(did, f"SN{did:04d}") for did in device_ids]
    )

def flusher(stop_event, q, PG_DSN, UPSERT_SQL, BATCH_SIZE, FLUSH_MS):
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
                                ensure_devices_exist(cur, batch)
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

def history_flusher(stop_event, history_q, PG_DSN, HISTORY_UPSERT_SQL, BATCH_SIZE, FLUSH_MS):
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
                                ensure_devices_exist(cur, batch)
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

def alarm_flusher(stop_event, alarm_q, PG_DSN, ALARM_UPSERT_SQL, BATCH_SIZE, FLUSH_MS):
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

def para_flusher(stop_event, para_q, PG_DSN, PARA_UPSERT_SQL, BATCH_SIZE, FLUSH_MS):
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
                                upsert_data = [
                                    {
                                        "device_id": rec["device_id"],
                                        "para": json.dumps(rec["para"]),
                                        "updated_at": rec["updated_at"]
                                    }
                                    for rec in batch
                                ]
                                execute_batch(cur, PARA_UPSERT_SQL, upsert_data, page_size=1000)
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

def rpc_ack_flusher(stop_event, rpc_ack_q, PG_DSN, RPC_ACK_UPDATE_SQL, BATCH_SIZE, FLUSH_MS):
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

def archive_alarm_worker(stop_event, archive_alarm_q, PG_DSN):
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