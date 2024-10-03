import socket 
import threading
import sys

HEADER = 64
CENTRAL_IP = socket.gethostbyname(socket.gethostname())
FORMAT = 'utf-8'
EXIT = "EXIT"

if len(sys.argv) != 4:
    print("Wrong number of arguments")
    exit()

PORT = int(sys.argv[1])
BROKER_IP = sys.argv[2]
BROKER_PORT = sys.argv[3]
ADDR = (CENTRAL_IP, PORT)
TAXI_IP = ""

taxi_list = []

def taxi_control():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()

    while True:
        conn, addr = server.accept()

        global TAXI_IP
        TAXI_IP = addr[0]

        msg = conn.recv(HEADER).decode(FORMAT) 
        if msg:
            try:  
                print(f"\rNew TAXI has been registered: [ID: {msg} ADDR: {addr}]\n$: ", end="")
                if int(msg) in taxi_list:
                    conn.send(f"0".encode(FORMAT))
                else:
                    conn.send(f"1".encode(FORMAT))
                    taxi_list.append(int(msg))
            finally:
                conn.send(f"0".encode(FORMAT))
            conn.close()

thread_taxi_control = threading.Thread(target=taxi_control)
thread_taxi_control.start()

def TAXI_ORDER(ORDER, TAXI_ID):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((TAXI_IP, 5051 + int(TAXI_ID)))

    msg = ORDER + " " + TAXI_ID
    message = msg.encode(FORMAT)

    client.send(message)

    print(client.recv(2048).decode(FORMAT))



def TAXI_GO(TAXI_ID, DEST):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((TAXI_IP, 5051 + int(TAXI_ID)))

    msg = f"GO {TAXI_ID} {DEST}"
    message = msg.encode(FORMAT)

    client.send(message)

    print(client.recv(2048).decode(FORMAT))

Working = True
command = ""
while Working:
    command = input("$: ")
    commands = command.split(" ")

    if(command == EXIT):
        Working = False
    elif len(commands) > 1 and int(commands[1]) not in taxi_list:
        print("Wrong taxi number")
    elif len(commands) == 2 and commands[0] in ("STOP", "RESUME", "RETURN") and commands[1].isdigit():
        TAXI_ORDER(commands[0], commands[1])
    elif len(commands) == 3 and commands[0] == "GO" and commands[1].isdigit():
        TAXI_GO(commands[1], commands[2])
    else:
        print("Wrong command")