import random
import sys
import time
import threading
from kafka import KafkaProducer, KafkaConsumer

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

def request_status_receive():
    global status
    global position

    for message in request_consumer:
        msg_split = message.split(" ")

        if msg_split[0] == ID:
            if msg_split[1] == "KO" and len(msg_split) == 2:
                return "KO"
            elif msg_split[1] == "KO" and len(msg_split) == 4:
                position = [msg_split[2], msg_split[3]]
                return "KO"
            elif msg_split[1] == "AT_CLIENT":
                status = "MOVING"
            elif msg_split[1] == "FINAL":
                return "FINAL"
            elif msg_split[1] == "ABORT":
                return "ABORT"
            


request = ""
working = True

request_producer = KafkaProducer(bootstrap_servers=ADDR_BROKER)

while working:
    request = input("Where do you want to go?: ")

    if request == "EXIT":
        request_producer.send("Request", f"EXIT {ID}".encode(FORMAT))
        working = False
    elif len(request.split(" ")):
        request_split = request.split(" ")
        if 1 <= int(request_split[0]) <= 20 and 1 <= int(request_split[1]) <= 20:
            status = "WAITING"
            while status != "FINAL":
                request_producer.send("Request", f"{ID} {request_split[0]} {request_split[1]}".encode(FORMAT))

                request_status = request_status_receive()

                if request_status == "KO":
                    status = "WAITING"
                elif request_status == "FINAL":
                    status = "FINAL"
                    position = [request_split[0], request_split[1]]
                elif request_status == "ABORT":
                    print("There are no free TAXIS")
                    status = "FINAL"
                
        else:
            print("Wrong destination")
