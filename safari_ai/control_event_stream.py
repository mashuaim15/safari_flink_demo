import threading
from kafka import KafkaProducer
import json
import time

def manual_event_stream(topic):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    while True:
        event = input("Enter event (start/close) or 'q' to quit: ").lower()
        if event == 'q':
            break
        if event in ['start', 'close']:
            data = {
                'name': 'manual_event',
                'value': event,
                'timestamp': time.time()
            }
            producer.send(topic, data)
            print(f"Sent manual event: {data}")
        else:
            print("Invalid input. Please enter 'start' or 'close'.")
    
    producer.close()


manual_thread = threading.Thread(target=manual_event_stream, args=('manual_event_topic',))
threads.append(manual_thread)


try:
    for thread in threads:
        thread.start()
    
    # Keep the main thread alive and handle manual input
    while True:
        time.sleep(0.1)  
except KeyboardInterrupt:
    print("Producers stopped by user")
finally:
    producer.close()