from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import numpy as np
from config import parameters

KAFKA_TOPIC_NAME = parameters["KAFKA_TOPIC_NAME"]
KAFKA_BOOTSTRAP_SERVER = parameters["KAFKA_BOOTSTRAP_SERVER"]

if __name__ == "__main__":
    st = time.time()
    print("Program started... @ ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    region = ["USA", "EMEA", "LATAM", "ASIA"]

    message = None
    for i in range(100000):
        i = i + 1
        message = {}

        message["log_id"] = str(i)
        message["log_region"] = str(np.random.choice(region))
        message["log_size"] = np.random.randint(100)
        message["log_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        kafka_producer_obj.send(KAFKA_TOPIC_NAME, message)

et = time.time()
elapsed_time = et - st
print('Execution time:', elapsed_time, 'seconds')
print("Finished.", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))