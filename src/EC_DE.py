import socket 
import threading
import time
import sys
from kafka import KafkaProducer, KafkaConsumer

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

active_request_ID = 0

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

closed = False
def send_taxi_state():
    while not closed:
        producer.send("TaxiStatus", (f"{ID} {state}").encode(FORMAT))
        time.sleep(1)

def send_taxi_position():
    while not closed:
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
                
            if mes[0] == "RESUME" and state != "MOVING":
                state = "FINAL"
                conn.send(f"TAXI NR {ID} has resumed it's working".encode(FORMAT))
            elif mes[0] == "STOP":
                state = "STOPPED"
                conn.send(f"TAXI NR {ID} has stopped".encode(FORMAT))
            elif mes[0] == "GO":
                if state == "MOVING":
                    conn.send(f"TAXI NR {ID} is already moving".encode(FORMAT))
                elif state == "STOPPED":
                    conn.send(f"TAXI NR {ID} has been stopped and can't move".encode(FORMAT))
                elif not (1 <= int(mes[2]) <= 20 and 1 <= int(mes[3]) <= 20):
                    conn.send(f"Wrong coordinates".encode(FORMAT))
                else:
                    conn.send(f"TAXI NR {ID} is going to [{mes[2]},{mes[3]}] point".encode(FORMAT))
                    TAXI_go([int(mes[2]),int(mes[3])])
                    if state != "STOPPED":
                        state = "FINAL"
            elif mes[0] == "RETURN":
                TAXI_go([1,1])
                conn.send(f"TAXI NR {ID} is returning to base".encode(FORMAT))
            else:
                conn.send(f"Ooops, something gone wrong, nothing happend".encode(FORMAT))

            connected = False
    conn.close()

def TAXI_go(dest):
    global position
    global state

    state = "MOVING"
    while not (int(dest[0]) == int(position[0]) and int(dest[1]) == int(position[1])):
        if state == "STOPPED":
            break

        if int(dest[0]) > int(position[0]):
            position[0]+=1
        elif int(dest[0]) < int(position[0]):
            position[0]-=1
        if int(dest[1]) > int(position[1]):
            position[1]+=1
        elif int(dest[1]) < int(position[1]):
            position[1]-=1

        time.sleep(1)

thread_stop = True
request_consumer = KafkaConsumer("TaxiRequest", bootstrap_servers=ADDR_BROKER)
def TAXI_request_receive():
    global active_request_ID
    global state
    global thread_stop
    for message in request_consumer:
        msg_split = message.value.decode(FORMAT).split(" ")

        if msg_split[1] == ID:

            active_request_ID = msg_split[0]
            src = [msg_split[2], msg_split[3]]
            dest = [msg_split[4], msg_split[5]]

            thread_status_send_OK = threading.Thread(target=lambda: send_request_status("OK"))

            #send_request_status thread OK
            thread_status_send_OK.start()
            TAXI_go(src)

            if state == "STOPPED":
                #send_request_status thread.stop
                thread_stop = False
                thread_status_send_OK.join()
                #send_request_status KO
                send_request_status("KO")
            else:
                send_request_status("AT_CLIENT")
                time.sleep(3)
                #send_request_status ZALADOWAL

                #send_request_status thread OK
                TAXI_go(dest)

                if state == "STOPPED":
                    #send_request_status thread.stop
                    thread_stop = False
                    thread_status_send_OK.join()
                    #send_request_status KO
                    send_request_status(f"KO {position[0]} {position[1]}")
                else:
                    #send_request_status DOJECHAL
                    thread_stop = False
                    thread_status_send_OK.join()
                    send_request_status(f"FINAL {position[0]} {position[1]}")
                    state = "FINAL"

thread_request_receive = threading.Thread(target=TAXI_request_receive)
thread_request_receive.start()

def send_request_status(request_status):
    global thread_stop
    if request_status == "OK":
        thread_stop = True
        while thread_stop:
            producer.send("RequestStatus", (f"{active_request_ID} {request_status}").encode(FORMAT))
            time.sleep(1)
    else:
        producer.send("RequestStatus", (f"{active_request_ID} {request_status}").encode(FORMAT))

    

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

##################################################

import win32api

def on_exit(signal_type):
    global state
    global closed

    state = "STOPPED"

    time.sleep(1)

    closed = True

    time.sleep(1)

    producer.send("TaxiStatus", (f"{ID} CLOSED").encode(FORMAT))

    time.sleep(1)


win32api.SetConsoleCtrlHandler(on_exit, True)

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