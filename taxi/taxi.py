import socket 
import threading
import time
import sys
#from kafka import KafkaProducer

HEADER = 64
PORT = 5050
TAXI_IP = socket.gethostbyname(socket.gethostname())
ADDR_CENT = ('192.168.1.137', 5051)
FORMAT = 'utf-8'
EXIT = "EXIT"

#producer= KafkaProducer(bootstrap_servers='127.0.0.1:9092')

if len(sys.argv) != 2:
    exit()


ID = sys.argv[1]

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

####################################################

if int(client.recv(2048).decode(FORMAT)):

    ADDR = (TAXI_IP, PORT+int(ID)+1)

    state = "OK"

    def send_taxi_state():
        while True:
            print(state)
            #producer.send("taxi_status", (ID + " " + state).encode(FORMAT))
            time.sleep(1)

    def taxi_warning(msg, sensor_id):
        print(f"Warning from sensor number {sensor_id}: {msg}")

        if msg != "HELLO":
            global state
            state = "KO"



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


    def start():
        server.listen()
        sensor_id = 1

        while True:
            conn, addr = server.accept()
            thread = threading.Thread(target=handle_sensor, args=(conn, sensor_id))
            thread.start()
            num_of_sensors = threading.active_count() - 2
            sensor_id+=1
            print(f"Number of sensors: {num_of_sensors}")

    ####################################################

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)

    print(f"[START] TAXI started at {TAXI_IP}, {PORT}")

    thread_status = threading.Thread(target=send_taxi_state)
    thread_status.start()

    start()
else:
    print("This taxi ID is taken")