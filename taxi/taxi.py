import socket 
import threading
import time

HEADER = 64
PORT = 5050
TAXI_IP = socket.gethostbyname(socket.gethostname())
ADDR = (TAXI_IP, PORT)
FORMAT = 'utf-8'
EXIT = "EXIT"

####################################################

def taxi_warning(msg, ID):
    print(f"Warning from sensor number {ID}: {msg}")

def handle_sensor(conn, ID):
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == EXIT:
                connected = False
            else:
                taxi_warning(msg, ID)
                conn.send(f"TAXI has been warned".encode(FORMAT))
    print(f"CLOSING THE SENSOR NUMBER {ID}")
    conn.close()


def start():
    server.listen()
    id = 1

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_sensor, args=(conn, id))
        thread.start()
        num_of_sensors = threading.active_count() - 1
        id+=1
        print(f"Number of sensors: {num_of_sensors}")

####################################################

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

print(f"[START] TAXI started at {TAXI_IP}, {PORT}")

start()