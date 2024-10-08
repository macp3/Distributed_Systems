from kafka import KafkaConsumer
import sys
import json
import threading
import time

FORMAT = 'utf-8'

BROKER_IP = sys.argv[1]
BROKER_PORT = sys.argv[2]

position_consumer = KafkaConsumer("CentralMap", bootstrap_servers=f"{BROKER_IP}:{BROKER_PORT}")

taxi_dic = {} # {'1' : ["FINAL", [1,1]], '2' : ["FINAL", [1,1]]}
customer_dic = {} 

def position_receive():
    global taxi_dic
    global customer_dic
    for message in position_consumer:
        message_split = message.value.decode(FORMAT).split("@")
        print(message_split[0].replace("\'", "\""))
        print(message_split[1].replace("\'", "\""))
        taxi_dic = json.loads(message_split[0].replace("\'", "\""))
        customer_dic = json.loads(message_split[1].replace("\'", "\""))
def print_map():
    while True:
        print(taxi_dic)
        print(customer_dic)
        time.sleep(1)
        

thread_position_receive = threading.Thread(target=position_receive)
thread_position_receive.start()

#print_map()
