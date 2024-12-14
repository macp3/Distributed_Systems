import socket 
import threading
import sys
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import time
import os
import json
import requests

HEADER = 64
CENTRAL_IP = socket.gethostbyname(socket.gethostname())
FORMAT = 'utf-8'
EXIT = "EXIT"

if len(sys.argv) != 5:
    print("Wrong number of arguments")
    exit()

PORT = int(sys.argv[1])
BROKER_IP = sys.argv[2]
BROKER_PORT = sys.argv[3]
CTC_IP = "127.0.0.1" # DO ZMIANY
#CTC_IP = sys.argv[4]
ADDR = (CENTRAL_IP, PORT)
TAXI_IP = ""

producer= KafkaProducer(bootstrap_servers=f"{BROKER_IP}:{BROKER_PORT}")

weather_status = "ON"
weather = 10
#taxi dic = {ID : [STATUS, [POSITION], [DESTINATION]]}
taxi_dic = {}
customer_dic = {}
position_dic = {}
with open("EC_locations.json") as pos_json:
    position_dic_file = json.load(pos_json)['locations']

    for a in position_dic_file:
        position_dic[a['Id']] = [int(a['POS'].split(",")[0]), int(a['POS'].split(",")[1])]

# ID [DEST]
request_queue = []

def taxi_control():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()

    while True:
        conn, addr = server.accept()

        global TAXI_IP
        TAXI_IP = addr[0]

        msg = conn.recv(HEADER).decode(FORMAT) 
        if msg:
            try:  
                taxi_list_file = pd.read_csv("taxis.csv")

                if int(msg) in taxi_list_file["ID"].values and msg not in taxi_dic.keys():
                    print(f"\rNew TAXI has been registered: [ID: {msg} ADDR: {addr}]\n\r$: ", end="")
                    producer.send("notifications", f"[{time.localtime().tm_mday}-{time.localtime().tm_mon}-{time.localtime().tm_year},{time.localtime().tm_hour}:{time.localtime().tm_min}] New TAXI has been registered: [ID: {msg} ADDR: {addr}])".encode(FORMAT))
                    conn.send(f"1".encode(FORMAT))
                    taxi_dic[msg] = ["FINAL", [1,1], [1,1]]
                else:
                    conn.send(f"0".encode(FORMAT))
            finally:
                conn.send(f"0".encode(FORMAT))
            conn.close()

thread_taxi_control = threading.Thread(target=taxi_control)
thread_taxi_control.start()

taxi_status_consumer = KafkaConsumer("TaxiStatus", bootstrap_servers=f"{BROKER_IP}:{BROKER_PORT}")
position_consumer = KafkaConsumer("TaxiAndCustomerCoordinates", bootstrap_servers=f"{BROKER_IP}:{BROKER_PORT}")


def taxi_status_receive():
    global taxi_dic
    for message in taxi_status_consumer:
        msg_split = message.value.decode(FORMAT).split(" ")
        # msg_split = 1 FINNAL
        if msg_split[0] not in taxi_dic.keys():
            taxi_dic[msg_split[0]] = ["FINAL", [1,1], [1,1]]

        if msg_split[1] == "CLOSED":
            taxi_dic.pop(msg_split[0])
        else:
            taxi_dic[msg_split[0]][0] = msg_split[1]


request_consumer = KafkaConsumer("Request", bootstrap_servers=f"{BROKER_IP}:{BROKER_PORT}")
def request_receive():
    for message in request_consumer:
        msg_split = message.value.decode(FORMAT).split(" ")
        #request_queue = [ID, [DEST]]
        #request_queue = [[1, [5,2]], [2, [6,8]], ...]
        if msg_split[0] == "EXIT":
            customer_dic.pop(msg_split[1])
        else:
            try:
                request_queue.append([int(msg_split[0]), [position_dic[msg_split[1]][0], position_dic[msg_split[1]][1]]])
            except:
                request_producer.send("RequestStatus", (f"{int(msg_split[0])} WRONG_DEST").encode(FORMAT))

#moje tez
thread_request_receive = threading.Thread(target=request_receive)
thread_request_receive.start()

#to tez moje
request_producer = KafkaProducer(bootstrap_servers=f"{BROKER_IP}:{BROKER_PORT}")

def handle_request():
    global taxi_dic
    while True:
        if not len(request_queue) == 0:
            found = False
            for taxiID, taxi_info in taxi_dic.items():
                if taxi_info[0] == "FINAL":
                    send_request_to_taxi(taxiID)
                    found = True
                    break
            if not found:
                time.sleep(1)
                request_producer.send("RequestStatus", (f"{request_queue[0][0]} ABORT").encode(FORMAT))
                request_queue.pop(0)

        time.sleep(1)

thread_request_handle = threading.Thread(target=handle_request)
thread_request_handle.start()

def send_request_to_taxi(taxiID):
    customerID = request_queue[0][0]
    try:
        message = f"{str(request_queue[0][0])} {str(taxiID)} {customer_dic[str(request_queue[0][0])][1][0]} {customer_dic[str(request_queue[0][0])][1][1]} {str(request_queue[0][1][0])} {str(request_queue[0][1][1])}"
        # taxiID x y

        request_producer.send("TaxiRequest", message.encode(FORMAT))

        request_queue.pop(0)
    except:
        pass

#customer_dic["3"][1] = #[5,9]

thread_taxi_status_receive = threading.Thread(target=taxi_status_receive)
thread_taxi_status_receive.start()

def position_receive():
    global taxi_dic
    global customer_dic
    for message in position_consumer:
        msg_split = message.value.decode(FORMAT).split(" ")
        # msg_split = TAXI 1 [1,1] A
        # msg_split = CUSTOMER 1 STATE [1,1] A
        if msg_split[0] == "TAXI":
            if msg_split[1] not in taxi_dic.keys():
                taxi_dic[msg_split[1]] = ["FINAL", [1,1], [1,1]]
            taxi_dic[msg_split[1]][1] = [int(msg_split[2][1:len(msg_split[2])-1].split(",")[0]), int(msg_split[2][1:len(msg_split[2])-1].split(",")[1])]
            taxi_dic[msg_split[1]][2] = [int(msg_split[3][1:len(msg_split[3])-1].split(",")[0]), int(msg_split[3][1:len(msg_split[3])-1].split(",")[1])]
        elif msg_split[0] == "CUSTOMER":
            customer_dic[msg_split[1]] = [msg_split[2], [int(msg_split[3][1:len(msg_split[3])-1].split(",")[0]), int(msg_split[3][1:len(msg_split[3])-1].split(",")[1])], msg_split[4]]

thread_taxi_position_receive = threading.Thread(target=position_receive)
thread_taxi_position_receive.start()


central_producer = KafkaProducer(bootstrap_servers=f"{BROKER_IP}:{BROKER_PORT}")

def taxi_position_send_map():
    while True:
        central_producer.send("CentralMap", f"{str(taxi_dic)}@{str(customer_dic)}".encode(FORMAT))
        time.sleep(1)

thread_taxi_position_send_map = threading.Thread(target=taxi_position_send_map)
thread_taxi_position_send_map.start()

def TAXI_ORDER(ORDER, TAXI_ID):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((TAXI_IP, 5051 + int(TAXI_ID)))

    msg = ORDER + " " + TAXI_ID
    message = msg.encode(FORMAT)

    client.send(message)

    print(client.recv(2048).decode(FORMAT))



def TAXI_GO(TAXI_ID, DEST):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((TAXI_IP, 5051 + int(TAXI_ID)))

    msg = f"GO {TAXI_ID} {DEST[0]} {DEST[1]}"
    message = msg.encode(FORMAT)

    client.send(message)

    print(client.recv(2048).decode(FORMAT))

def check_weather():
    global weather
    global weather_status
    stopped = False
    while True:
        time.sleep(10)
        try:
            weather = requests.get(f"http://{CTC_IP}:6000/weather").json()['temp']
            producer.send("notifications", f"[{time.localtime().tm_mday}-{time.localtime().tm_mon}-{time.localtime().tm_year},{time.localtime().tm_hour}:{time.localtime().tm_min}] Temperature: {weather}".encode(FORMAT))
            weather_status = "ON"
            if weather < 0:
                stopped = True
                for taxi_id in taxi_dic.keys():
                    TAXI_ORDER("RETURN", taxi_id)
            elif stopped and weather >= 0:
                stopped = False
                for taxi_id in taxi_dic.keys():
                    TAXI_ORDER("RESUME", taxi_id)
        except:
            weather_status = "OFF"



thread_check_weather = threading.Thread(target=check_weather)
thread_check_weather.start()

def set_city(city):
    try:
        requests.post(f"http://{CTC_IP}:6000/weather", json={"city" : city})
    except:
        print("Wrong city name")

#reset requestow
for customers_id in range(100):
        request_producer.send("RequestStatus", (f"{customers_id} ABORT").encode(FORMAT))


Working = True
command = ""
while Working:
    command = input("$: ")
    commands = command.split(" ")

    if(command == EXIT):
        Working = False
    elif len(commands) == 2 and commands[0] == "CITY":
        set_city(commands[1])
    elif len(commands) > 1 and commands[1] not in taxi_dic.keys():
        print("Wrong taxi number")
    elif len(commands) == 2 and commands[0] in ("STOP", "RESUME", "RETURN") and commands[1].isdigit():
        TAXI_ORDER(commands[0], commands[1])
    elif len(commands) == 4 and commands[0] == "GO" and commands[1].isdigit() and commands[2].isdigit() and commands[3].isdigit():
        TAXI_GO(commands[1], [commands[2],commands[3]])
    else:
        print("Wrong command")