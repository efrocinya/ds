import pandas as pd
import os
import json
from confluent_kafka import Producer
from dotenv import load_dotenv


def delivery_report(err, msg):
    if err is not None:
        print(f"error Kafka: {msg}")

def send_to_kafka():

    producer = Producer({"bootstrap.servers":kafka_bootstrap})
    total = 0

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size): 
        #Убрать ограничение! 
        if total > 10000:
            break 
        for record in chunk.to_dict(orient="records"):
            value = json.dumps(record, ensure_ascii=False).encode("utf-8")
            producer.produce(topik, value=value, callback=delivery_report)
            producer.poll(0)

        total += len(chunk)
        print(f"Total: {total}")
    producer.flush()

if __name__ == "__main__":
    load_dotenv()

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topik = os.getenv("KAFKA_TOPIC", "csv-data")
    csv_path = os.getenv("CSV_PATH", "./ds/input/output.csv")
    chunk_size = int(os.getenv("CHUNK_SIZE", "2000"))
    send_to_kafka()

