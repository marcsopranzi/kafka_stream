from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import numpy as np

KAFKA_TOPIC_NAME = 'log-topic'
KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'

if __name__ == "__main__":
    print("Program started... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    region = ["USA", "EMEA", "LATAM", "ASIA"]

    message = None
    for i in range(500):
        i = i + 1
        message = {}

        message["log_id"] = str(i)
        message["log_region"] = str(np.random.choice(region))
        message["log_size"] = np.random.randint(100)
        message["log_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        kafka_producer_obj.send(KAFKA_TOPIC_NAME, message)
        print("Message sent: ", message)
        time.sleep(1)

print("Finished.")