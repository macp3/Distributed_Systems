import socket
import sys
import threading
import time

HEADER = 64
FORMAT = 'utf-8'
EXIT = "EXIT"

msg = "OK"
inp = "OK"

####################################################

def send():
    global msg
    global inp

    while inp != EXIT:

        message = msg.encode(FORMAT)
        msg_length = len(message)
        send_length = str(msg_length).encode(FORMAT)
        send_length += b' ' * (HEADER - len(send_length))
        client.send(send_length)
        client.send(message)

        time.sleep(1)


def send_exit(msg):
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
    print (f"Connection with TAXI: [{ADDR} ID: {PORT - 5051}]")

    thread_status = threading.Thread(target=send)
    thread_status.start()

    while inp != EXIT :
        inp=input()
        if inp != EXIT:
            if msg == "OK":
                msg = "KO"
            else:
                msg = "OK"

    print ("CLOSING SENSOR")
    send_exit(EXIT)
    client.close()
else:
    print (f"You're missing {3-sys.argv} argument(s)")