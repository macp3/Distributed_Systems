from kafka import KafkaConsumer
import sys
import json
import threading
import time
from colorama import Fore, Style
import os

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
        taxi_dic = json.loads(message_split[0].replace("\'", "\""))
        customer_dic = json.loads(message_split[1].replace("\'", "\""))

def draw_map():
    #Rysuje mapę 20x20 z taksówkami, klientami i destynacjami oraz wyświetla informacje poniżej mapy.
    while True:
        # Wyczyść konsolę (dla lepszej czytelności)
        os.system('cls' if os.name == 'nt' else 'clear')

        # Tworzenie nagłówka (od 1 do 20)
        header = '   ' + ' '.join([str(i) if len(str(i)) > 1 else " "+str(i) for i in range(1, 21)])
        print(header)

        # Tworzenie siatki mapy
        map_ = [['..' for _ in range(20)] for _ in range(20)]  # Siatka 20x20 zainicjalizowana kropkami

        # Umieszczanie taksówek na mapie
        for taxi_id, taxi_info in taxi_dic.items():
            try:
                pos = taxi_info[1]
                if isinstance(pos, list) and len(pos) == 2:
                    x, y = (pos[0] - 1) % 20, (pos[1] - 1) % 20
                    status = taxi_info[0]
                    if status == "MOVING":
                        # Zielone taksówki, które zakończyły podróż
                        map_[y][x] = Fore.GREEN + f'T{taxi_id}' + Style.RESET_ALL
                    elif status == "FINAL" or status == "STOPPED":
                        map_[y][x] = Fore.RED + f'T{taxi_id}' + Style.RESET_ALL
            except Exception as e:
                print(f"Error with taxi {taxi_id}: {e}")

        # Umieszczanie klientów na mapie
        for customer_id, customer_info in customer_dic.items():
            customer_status = customer_info[0]
            customer_pos = customer_info[1]
            try:
                if isinstance(customer_pos, list) and len(customer_pos) == 2 and customer_status != "MOVING":
                    x, y = (customer_pos[0]-1) % 20, (customer_pos[1]-1) % 20
                    # Niebiescy klienci
                    map_[y][x] = Fore.BLUE + f'C{customer_id}' + Style.RESET_ALL
            except Exception as e:
                print(f"Error with client {customer_id}: {e}")

        # Wydrukowanie każdej linii mapy
        for i, row in enumerate(map_, start=1):
            print(f'{i:2} ' + ' '.join(row))

        print("\n" + "-" * 50 + "\n")  # Separator po mapie

        # Wyświetlenie informacji o taksówkach poniżej mapy
        print(Fore.GREEN + "Information about taxis:" + Style.RESET_ALL)
        for taxi_id, taxi_info in taxi_dic.items():
            status, pos = taxi_info
            print(f"Taxi {taxi_id}: Status = {status}, Position = {pos}")

        # Wyświetlenie informacji o klientach poniżej mapy
        print(Fore.BLUE + "\nInformation about customers:" + Style.RESET_ALL)
        for customer_id, customer_info in customer_dic.items():
            print(f"Client {customer_id}: Position = {customer_info}")

        print("\n" + "-" * 50 + "\n")  # Separator

        # Opóźnienie odświeżania mapy
        time.sleep(1)
        

thread_position_receive = threading.Thread(target=position_receive)
thread_position_receive.start()

draw_map()
