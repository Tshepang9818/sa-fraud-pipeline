import json, os, psycopg2
from kafka import KafkaConsumer
from datetime import datetime, timezone

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sa_transactions")
DB_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@postgres/{os.getenv('POSTGRES_DB')}"

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS flagged_transactions (
        tx_id TEXT PRIMARY KEY,
        card_number TEXT,
        merchant TEXT,
        amount_zar FLOAT,
        city TEXT,
        reason TEXT,
        flagged_at TIMESTAMPTZ DEFAULT NOW()
    )
""")
conn.commit()

card_last_seen = {}

def detect_fraud(tx):
    reasons = []
    if tx["amount_zar"] > 10000:
        reasons.append("HIGH_AMOUNT")
    hour = datetime.fromisoformat(tx["timestamp"]).hour
    if 0 <= hour < 4:
        reasons.append("ODD_HOURS")
    card = tx["card_number"]
    now = datetime.now(timezone.utc)
    if card in card_last_seen:
        delta = (now - card_last_seen[card]).total_seconds()
        if delta < 60:
            reasons.append("VELOCITY_BREACH")
    card_last_seen[card] = now
    return reasons

consumer = KafkaConsumer(
    TOPIC, bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest", group_id="fraud-detector"
)

print("Detector running — watching for fraud...")
for msg in consumer:
    tx = msg.value
    reasons = detect_fraud(tx)
    if reasons:
        cur.execute("""
            INSERT INTO flagged_transactions (tx_id, card_number, merchant, amount_zar, city, reason)
            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
        """, (tx["tx_id"], tx["card_number"], tx["merchant"],
              tx["amount_zar"], tx["city"], ", ".join(reasons)))
        conn.commit()
        print(f"FLAGGED: {tx['tx_id']} | {', '.join(reasons)}")