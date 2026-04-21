import json, time, random
from datetime import datetime
from kafka import KafkaProducer
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sa_transactions")

SA_MERCHANTS = [
    "Checkers Menlyn", "Pick n Pay Sandton", "Woolworths V&A",
    "Shell Pretoria", "Engen Johannesburg", "Capitec ATM Durban",
    "Takealot Online", "Dischem Cape Town", "Mr Price Eastgate",
    "KFC Soweto", "Nandos Rosebank", "Steers Bloemfontein"
]

def generate_transaction():
    is_suspicious = random.random() < 0.08
    amount = round(random.uniform(5000, 50000), 2) if is_suspicious \
             else round(random.uniform(10, 3000), 2)
    return {
        "tx_id": f"TX{random.randint(100000, 999999)}",
        "card_number": f"4{random.randint(100,999)}-****-****-{random.randint(1000,9999)}",
        "merchant": random.choice(SA_MERCHANTS),
        "amount_zar": amount,
        "timestamp": datetime.utcnow().isoformat(),
        "is_suspicious": is_suspicious,
        "city": random.choice(["Johannesburg","Cape Town","Durban","Pretoria","Bloemfontein"])
    }

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Simulator running — publishing SA transactions...")
while True:
    tx = generate_transaction()
    producer.send(TOPIC, tx)
    print(f"Published: {tx['tx_id']} | R{tx['amount_zar']} | {tx['merchant']}")
    time.sleep(random.uniform(0.5, 2))