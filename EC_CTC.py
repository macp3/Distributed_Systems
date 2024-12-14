import os
from dotenv import load_dotenv
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

load_dotenv()

class EC_CTC:
    API_KEY = os.getenv("WEATHER_API_KEY")

    city = "alicante"

    def get_weather(self):
        response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?q={self.city}&units=metric&appid={self.API_KEY}")
        return {"temp" : response.json()['main']['temp']}
    
    def set_city(self, city):
        old_city = self.city
        try:
            self.city = city
            response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?q={self.city}&units=metric&appid={self.API_KEY}")
        except:
            self.city = old_city
            raise ValueError("Wrong city name")

weather = EC_CTC()

@app.get("/weather")
def get_weather_api():
    return jsonify(weather.get_weather())

@app.post("/weather")
def set_city():
    if request.is_json:
        city_json = request.get_json()
        weather.set_city(city_json['city'])
        return city_json, 201
    return {"error": "Request must be JSON"}, 415

app.run(host="localhost", port=6000)