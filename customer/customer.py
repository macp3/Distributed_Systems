import random
import sys
import time
import threading
from kafka import KafkaProducer, KafkaConsumer
import json

#######################################

import win32api

def on_exit(signal_type):
    global working
    request_producer.send("Request", f"EXIT {ID}".encode(FORMAT))
    working = False

win32api.SetConsoleCtrlHandler(on_exit, True)

#######################################

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

request_consumer = KafkaConsumer("RequestStatus", bootstrap_servers=ADDR_BROKER)

received = False

def request_status_receive():
    global status
    global position

    for message in request_consumer:
        msg_split = message.value.decode(FORMAT).split(" ")
        ###########################
        print(message.value.decode(FORMAT))
        ###########################
        if msg_split[0] == ID:
            if msg_split[1] == "KO" and len(msg_split) == 2:
                return "KO"
            elif msg_split[1] == "KO" and len(msg_split) == 4:
                position = [msg_split[2], msg_split[3]]
                return "KO"
            elif msg_split[1] == "AT_CLIENT":
                status = "MOVING"
            elif msg_split[1] == "FINAL":
                position = [msg_split[2], msg_split[3]]
                return "FINAL"
            elif msg_split[1] == "ABORT":
                return "ABORT"

request = ""
working = True

request_producer = KafkaProducer(bootstrap_servers=ADDR_BROKER)

request_queue = []
with open("EC_Request.json") as req_json:
    for a in json.load(req_json)["Requests"]:
        request_queue.append(a["Id"])

if len(request_queue):
    for requested_id in request_queue:
        received = False
        status = "WAITING"
        while status != "FINAL":
            request_producer.send("Request", f"{ID} {requested_id}".encode(FORMAT))

            request_status = request_status_receive()

            if request_status == "KO":
                status = "WAITING"
            elif request_status == "FINAL":
                status = "FINAL"
            elif request_status == "ABORT":
                time.sleep(5)
                status = "WAITING"
        time.sleep(4)

while working:
    request = input("Where do you want to go?: ")

    if request == "EXIT":
        request_producer.send("Request", f"EXIT {ID}".encode(FORMAT))
        working = False
    elif len(request.split(" ")):
        if len(request) == 1:
            received = False
            status = "WAITING"
            while status != "FINAL":
                request_producer.send("Request", f"{ID} {request}".encode(FORMAT))

                request_status = request_status_receive()

                if request_status == "KO":
                    status = "WAITING"
                elif request_status == "FINAL":
                    status = "FINAL"
                elif request_status == "ABORT":
                    time.sleep(5)
                    status = "WAITING"
        else:
            print("Wrong destination")
