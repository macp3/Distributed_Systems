import socket 
import threading
import sys
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import time
import os

HEADER = 64
CENTRAL_IP = socket.gethostbyname(socket.gethostname())
FORMAT = 'utf-8'
EXIT = "EXIT"

if len(sys.argv) != 4:
    print("Wrong number of arguments")
    exit()

PORT = int(sys.argv[1])
BROKER_IP = sys.argv[2]
BROKER_PORT = sys.argv[3]
ADDR = (CENTRAL_IP, PORT)
TAXI_IP = ""

taxi_dic = {}
customer_dic = {"1" : [6,8], "2": [5, 1]}

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
                    print(f"\rNew TAXI has been registered: [ID: {msg} ADDR: {addr}]\n$: ", end="")
                    conn.send(f"1".encode(FORMAT))
                    taxi_dic[msg] = ["FINAL", [1,1]]
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
        taxi_dic[msg_split[0]][0] = msg_split[1]

thread_taxi_status_receive = threading.Thread(target=taxi_status_receive)
thread_taxi_status_receive.start()

def position_receive():
    global taxi_dic
    for message in position_consumer:
        msg_split = message.value.decode(FORMAT).split(" ")
        # msg_split = TAXI 1 [1,1]
        # msg_split = CUSTOMER 1 [1,1]
        if msg_split[0] == "TAXI":
            taxi_dic[msg_split[1]][1] = [int(msg_split[2][1]), int(msg_split[2][3])]
        elif msg_split[0] == "CUSTOMER":
            pass

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

    msg = f"GO {TAXI_ID} {DEST}"
    message = msg.encode(FORMAT)

    client.send(message)

    print(client.recv(2048).decode(FORMAT))

Working = True
command = ""
while Working:
    command = input("$: ")
    commands = command.split(" ")

    if(command == EXIT):
        Working = False
    elif len(commands) > 1 and commands[1] not in taxi_dic.keys():
        print("Wrong taxi number")
    elif len(commands) == 2 and commands[0] in ("STOP", "RESUME", "RETURN") and commands[1].isdigit():
        TAXI_ORDER(commands[0], commands[1])
    elif len(commands) == 3 and commands[0] == "GO" and commands[1].isdigit():
        TAXI_GO(commands[1], commands[2])
    else:
        print("Wrong command")