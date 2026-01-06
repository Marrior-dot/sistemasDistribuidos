from socket import *
def middleware_connection(port, command:str, alcance:int):
    s = socket(AF_INET, SOCK_STREAM)
    s.connect(("localhost", port))
    if command != '':
        s.send(command)
        if alcance == 1:
            
