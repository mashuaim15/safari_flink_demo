import time
import random
from kafka import KafkaProducer
import json
import threading

bootstrap_servers = ['localhost:9092']


producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_status(name, fps, topic):
    while True:
        status = random.choice([0, 1])
        data = {
            'name': name,
            'value': status,
            'timestamp': time.time()
        }
        producer.send(topic, data)
        print(f"Sent {name}: {data}")
        time.sleep(1 / fps)

def generate_value(name, fps, topic):
    while True:
        value = random.uniform(0, 100)
        data = {
            'name': name,
            'value': value,
            'timestamp': time.time()
        }
        producer.send(topic, data)
        print(f"Sent {name}: {data}")
        time.sleep(1 / fps)

# 4 data stream A,B,C,D
threads = [
    threading.Thread(target=generate_status, args=('status_A', 10, 'status_A_topic')),
    threading.Thread(target=generate_status, args=('status_B', 7, 'status_B_topic')),
    threading.Thread(target=generate_value, args=('value_C', 15, 'value_C_topic')),
    threading.Thread(target=generate_value, args=('value_D', 20, 'value_D_topic'))
]

# Start all threads
for thread in threads:
    thread.start()

try:
    # Keep the main thread alive
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Producers stopped by user")
finally:
    producer.close()