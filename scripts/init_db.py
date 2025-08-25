import os
import psycopg2
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(".env"), override=True)

def main():
    user = os.getenv("POSTGRES_USER")
    pwd  = os.getenv("POSTGRES_PASSWORD")
    db   = os.getenv("POSTGRES_DB")
    port = os.getenv("PG_PORT", "5432")
    host = "localhost"  # 宿主机映射端口

    dsn = f"host={host} port={port} dbname={db} user={user} password={pwd}"
    sql_path = os.path.join(os.path.dirname(__file__), "..", "init", "schema.sql")
    sql_path = os.path.abspath(sql_path)

    with open(sql_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    print(f"[init_db] connecting {dsn}")
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
    print("[init_db] schema applied successfully")

if __name__ == "__main__":
    main()
