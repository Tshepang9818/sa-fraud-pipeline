from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
import psycopg2, os

app = FastAPI(title="SA Fraud Pipeline API")
Instrumentator().instrument(app).expose(app)

DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres/{os.getenv('POSTGRES_DB')}"

def get_conn():
    return psycopg2.connect(DB_URL)

@app.get("/flagged")
def get_flagged(limit: int = 50):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM flagged_transactions ORDER BY flagged_at DESC LIMIT %s", (limit,))
    rows = cur.fetchall()
    conn.close()
    return [{"tx_id":r[0],"card":r[1],"merchant":r[2],"amount_zar":r[3],"city":r[4],"reason":r[5],"flagged_at":str(r[6])} for r in rows]

@app.get("/stats")
def get_stats():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), reason FROM flagged_transactions GROUP BY reason")
    rows = cur.fetchall()
    conn.close()
    return {"counts_by_reason": {r[1]: r[0] for r in rows}}