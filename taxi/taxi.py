import socket 
import threading
import time
import sys
from kafka import KafkaProducer

HEADER = 64
PORT = 5050
TAXI_IP = socket.gethostbyname(socket.gethostname())
FORMAT = 'utf-8'
EXIT = "EXIT"

if len(sys.argv) != 6:
    exit()

ADDR_CENT = (sys.argv[1], int(sys.argv[2]))
ADDR_BROKER = f"{sys.argv[3]}:{sys.argv[4]}"
ID = sys.argv[5]

producer= KafkaProducer(bootstrap_servers=ADDR_BROKER)

state = "FINAL"
position = [1,1]

connected_sensors = []

def send(msg):
    message = msg.encode(FORMAT)
    # msg_length = len(message)
    # send_length = str(msg_length).encode(FORMAT)
    # send_length += b' ' * (HEADER - len(send_length))
    # client.send(send_length)
    client.send(message)

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(ADDR_CENT)
send(str(ID))

def send_taxi_state():
        while True:
            producer.send("TaxiStatus", (f"{ID} {state}").encode(FORMAT))
            time.sleep(1)

def send_taxi_position():
    while True:
            producer.send("TaxiAndCustomerCoordinates", (f"TAXI {ID} [{position[0]},{position[1]}]").encode(FORMAT))
            time.sleep(1)

def taxi_warning(msg, sensor_id):
    global state

    if msg == "KO":
        print(f"Warning from sensor number {sensor_id}")
        state = "STOPPED"


def handle_sensor(conn, sensor_id):
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == EXIT:
                connected = False
            else:
                taxi_warning(msg, sensor_id)
                conn.send(f"TAXI has been warned".encode(FORMAT))
    print(f"CLOSING THE SENSOR NUMBER {sensor_id}")
    conn.close()

def handle_central(conn, addr):
    connected = True
    while connected:
        msg = conn.recv(HEADER).decode(FORMAT)
        if msg:
            mes = msg.split(" ")
            global state
                
            if mes[0] == "RESUME":
                state = "FINAL"
                conn.send(f"TAXI NR {ID} has resumed it's working".encode(FORMAT))
            elif mes[0] == "STOP":
                state = "STOPPED"
                conn.send(f"TAXI NR {ID} has stopped".encode(FORMAT))
            elif mes[0] == "GO":
                TAXI_go(mes[2])
                conn.send(f"TAXI NR {ID} is going to {mes[2]} point".encode(FORMAT))
            elif mes[0] == "RETURN":
                TAXI_go([0,0])
                conn.send(f"TAXI NR {ID} is returning to base".encode(FORMAT))
            else:
                conn.send(f"Ooops, something gone wrong, nothing happend".encode(FORMAT))

            connected = False
    conn.close()

def TAXI_go(dest):
    pass

def start():
    server.listen()
    sensor_id = 1

    while True:
        conn, addr = server.accept()

        # MOZE NIE DZIALAC - przetestowac z 2 ip
        if addr[0] == ADDR_CENT[0]:
            thread_cen = threading.Thread(target=handle_central, args=(conn, addr))
            thread_cen.start()
        else:
            thread = threading.Thread(target=handle_sensor, args=(conn, sensor_id))
            thread.start()
            connected_sensors.append(sensor_id)

            sensor_id+=1
            print(f"Number of sensors: {len(connected_sensors)}")

####################################################

if int(client.recv(2048).decode(FORMAT)):

    ADDR = (TAXI_IP, PORT+int(ID)+1)

####################################################
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)

    print(f"[START] TAXI started at {ADDR[0]}, {ADDR[1]}")

    thread_status = threading.Thread(target=send_taxi_state)
    thread_status.start()

    thread_position = threading.Thread(target=send_taxi_position)
    thread_position.start()

    start()
else:
    print("This taxi ID is not valid")