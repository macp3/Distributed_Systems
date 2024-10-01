import socket
import sys

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
EXIT = "EXIT"

####################################################

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)

####################################################

if  (len(sys.argv) == 3):
    TAXI_IP = sys.argv[1]
    PORT = int(sys.argv[2])
    ADDR = (TAXI_IP, PORT)
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print (f"Connection with TAXI: [{ADDR}]")
    msg = "HELLO"

    while msg != EXIT :
        send(msg)
        print("Response from taxi: ", client.recv(2048).decode(FORMAT))
        msg=input()

    print ("CLOSING SENSOR")
    send(EXIT)
    client.close()
else:
    print (f"You're missing {3-sys.argv} argument(s)")