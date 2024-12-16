from flask import Flask, request, jsonify
import socket 
import pandas as pd
from flask_cors import CORS
from kafka import KafkaConsumer, KafkaProducer
import threading
from cryptography.fernet import Fernet 
import sys

CENTRAL_IP = socket.gethostbyname(socket.gethostname())

if len(sys.argv) != 3:
    print("Wrong number of arguments")
    exit()

BROKER_IP = sys.argv[1]
BROKER_PORT = sys.argv[2]
FORMAT = 'utf-8'

app = Flask(__name__)

CORS(app)

info = {}
notifications = []

@app.put("/register")
def register_taxi():
    taxi_list_file = pd.read_csv("taxis.csv")
    
    taxi_list_file.loc[len(taxi_list_file)] = [request.args["ID"],request.args["key"]]

    print(taxi_list_file)
    taxi_list_file.to_csv("./taxis.csv", sep=',', encoding='utf-8', index=False, header=True)

    return "Taxi has been registered"

json_id = 0
@app.put("/receive_info")
def receive_info():
    global info
    global json_id
    info = request.json
    json_id += 1
    return "OK"

@app.get("/db")
def get_db():
    return pd.read_csv("taxis.csv").to_json()

@app.get("/front")
def get_info():
    info['notification'] = notifications
    info['json_id'] = json_id
    return jsonify(info)

def get_tokens_list():
    taxi_list_file = pd.read_csv("taxis.csv")

    return taxi_list_file['key'].values.tolist()

notification_consumer = KafkaConsumer("notifications", bootstrap_servers=f"{BROKER_IP}:{BROKER_PORT}")
def notifications_receive():
    global notifications
    for message in notification_consumer:
        msg_enc = message.value

        tokens = get_tokens_list()
        for token in tokens:
            try:
                msg_dec = Fernet(token).decrypt(msg_enc)

                notifications.insert(0, msg_dec.decode(FORMAT))
            except:
                pass

thread_notifications_receive = threading.Thread(target=notifications_receive)
thread_notifications_receive.start()

app.run(host=CENTRAL_IP, port=6002)