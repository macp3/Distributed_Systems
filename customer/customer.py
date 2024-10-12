import random
import sys
import time
import threading
from kafka import KafkaProducer

FORMAT = 'utf-8'

if len(sys.argv) != 4:
    exit()

ADDR_BROKER = f"{sys.argv[1]}:{sys.argv[2]}"
ID = sys.argv[3]
status = "FINAL"

producer= KafkaProducer(bootstrap_servers=ADDR_BROKER)

position = [random.randint(1, 20), random.randint(1, 20)]

def send_customer_position():
    while True:
        producer.send("TaxiAndCustomerCoordinates", (f"CUSTOMER {ID} {status} [{position[0]},{position[1]}]").encode(FORMAT))
        time.sleep(1)

thread_customer_position_send = threading.Thread(target=send_customer_position)
thread_customer_position_send.start()