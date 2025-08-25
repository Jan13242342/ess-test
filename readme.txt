建表（只需一次）

cd your-project\scripts
py -m pip install --user psycopg2-binary python-dotenv
py init_db.py


跑 API（本地）

cd ..\api
py -m pip install --user -r requirements.txt
py -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload


跑 ingestor（本地）

cd ..\ingestor
py -m pip install --user -r requirements.txt
py main.py