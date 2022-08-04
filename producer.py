from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import numpy as np

KAFKA_TOPIC_NAME_CONS = 'test'
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    region = ["USA", "EMEA", "LATAM", "ASIA"]

    message = None
    for i in range(500):
        i = i + 1
        message = {}
        print("Sending message to Kafka topic: " + str(i))
        event_datetime = datetime.now()

        message["transaction_id"] = str(i)
        message["transaction_region"] = str(np.random.choice(region))
        message["transaction_size"] = np.random.randint(100)
        message["transaction_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")

        print("Message to be sent: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)
print("Kafka Producer Application Completed. ")