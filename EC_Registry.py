import os
from dotenv import load_dotenv
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

class EC_Registry:
    pass

registry = EC_Registry()

@app.put("/register")
def put():
    pass

@app.delete("/delete")
def delete():
    pass

app.run(host="localhost", port=6001)