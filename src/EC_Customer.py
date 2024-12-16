import random
import sys
import time
import threading
from kafka import KafkaProducer, KafkaConsumer
import json

FORMAT = 'utf-8'

if len(sys.argv) != 4:
    exit()

ADDR_BROKER = f"{sys.argv[1]}:{sys.argv[2]}"
ID = sys.argv[3]
status = "FINAL"

producer= KafkaProducer(bootstrap_servers=ADDR_BROKER)

position = [random.randint(1, 20), random.randint(1, 20)]
destination = "0"

closed = False
def send_customer_position():
    while not closed:
        producer.send("CustomerCoordinates", (f"CUSTOMER {ID} {status} [{position[0]},{position[1]}] {destination}").encode(FORMAT))
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

        if msg_split[0] == ID:
            if msg_split[1] == "KO" and len(msg_split) == 2:
                return "KO"
            elif msg_split[1] == "KO" and len(msg_split) == 4:
                position = [msg_split[2], msg_split[3]]
                print(f"KO status of request - leaving taxi at {position}")
                return "KO"
            elif msg_split[1] == "AT_CLIENT":
                print("Entering TAXI")
                status = "MOVING"
            elif msg_split[1] == "FINAL":
                print(f"Reached destination: {destination}")
                position = [msg_split[2], msg_split[3]]
                return "FINAL"
            elif msg_split[1] == "ABORT":
                return "ABORT"
            elif msg_split[1] == "WRONG_DEST":
                return "WRONG_DEST"

request = ""
working = True

request_producer = KafkaProducer(bootstrap_servers=ADDR_BROKER)

request_queue = []
with open("EC_Request.json") as req_json:
    for a in json.load(req_json)["Requests"]:
        request_queue.append(a["Id"])

print(f"CUSTOMER {ID}")

#######################################

import win32api

def on_exit(signal_type):
    global working
    global closed
    closed = True
    request_producer.send("Request", f"EXIT {ID}".encode(FORMAT))
    time.sleep(1)

win32api.SetConsoleCtrlHandler(on_exit, True)

#######################################

if len(request_queue):
    time.sleep(1)
    for requested_id in request_queue:
        received = False
        status = "WAITING"
        while status != "FINAL":
            request_producer.send("Request", f"{ID} {requested_id}".encode(FORMAT))
            producer.send("notifications", (f"[{time.localtime().tm_mday}-{time.localtime().tm_mon}-{time.localtime().tm_year},{time.localtime().tm_hour}:{time.localtime().tm_min}] CUSTOMER {ID} sent request to go to {requested_id}").encode(FORMAT))
            destination = requested_id

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
            destination = request
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
                elif request_status == "WRONG_DEST":
                    print("Wrong destination")
                    status = "FINAL"
        else:
            print("Wrong destination")
