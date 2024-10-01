import socket 
import threading

HEADER = 64
PORT = 5051
CENTRAL_IP = socket.gethostbyname(socket.gethostname())
ADDR = (CENTRAL_IP, PORT)
FORMAT = 'utf-8'
EXIT = "EXIT"

taxi_list = []

def taxi_control():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()

    while True:
        conn, addr = server.accept()

        msg = conn.recv(HEADER).decode(FORMAT) 
        if msg:
            try:  
                print(msg)
                if int(msg) in taxi_list:
                    conn.send(f"0".encode(FORMAT))
                else:
                    conn.send(f"1".encode(FORMAT))
                    taxi_list.append(int(msg))
            finally:
                conn.send(f"0".encode(FORMAT))
            conn.close()

# thread_taxi_control = threading.Thread(target=taxi_control)
# thread_taxi_control.start()
taxi_control()