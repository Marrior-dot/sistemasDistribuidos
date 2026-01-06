import socket
s = socket.socket()
port_mysql = 3306
port_mysql1 = 3307
port_mysql2 = 3308
s.connect(('localhost', port_mysql))
print("conectado")
s.close()