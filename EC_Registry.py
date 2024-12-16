import os
from dotenv import load_dotenv
import requests
from flask import Flask, request, jsonify
from cryptography.fernet import Fernet
import sys
import socket
hostname = socket.gethostname()
IPAddr = socket.gethostbyname(hostname)

if len(sys.argv) != 2:
    print("Wrong number of arguments")
    exit()

DB_IP = sys.argv[1]
FORMAT = 'utf-8'

app = Flask(__name__)

@app.put("/register")
def register_taxi():
    ID = request.args.get("ID")
    key = Fernet.generate_key()

    requests.put(f"http://{DB_IP}:6002/register?ID={ID}&key={key.decode(FORMAT)}")
    return jsonify(key.decode(FORMAT))


@app.delete("/delete")
def delete_taxi():
    pass

app.run(host=IPAddr, port=6001, ssl_context=('server.crt', 'server.key'))