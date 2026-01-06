# first of all import the socket library 
import socket             
from middleware import middleware_connection
# next create a socket object 
s = socket.socket()         
print ("Socket successfully created")

# reserve a port on your computer in our 
# case it is 12345 but it can be anything 
port = 3000           

# Next bind to the port 
# we have not typed any ip in the ip field 
# instead we have inputted an empty string 
# this makes the server listen to requests 
# coming from other computers on the network 
s.bind(('localhost', port))         
print ("socket binded to %s" %(port)) 

# put the socket into listening mode 
s.listen(5)     
print ("socket is listening")            

# a forever loop until we interrupt it or 
# an error occurs
 
while True: 

# Conexão estabelecida com o cliente. 
  c, addr = s.accept()     
  print ('Got connection from', addr )
  print("Escolha uma operação")
  
  operacao:int = int(input("Escolha:\n1-SELECT\n2-INSERT\n3-UPDATE\n4-DELETE"))

  valor:str=''
  #Usado para inserir ou deletar
  if operacao == 2 or operacao == 3:
    valor = str(input("Escolha o valor"))

  #Usado para deletar um valor
  elif operacao == 4:
    valor = str(input("Escolha um valor existente"))

  #Escolher entre Unicast, BroadCast ou Multicast
  print("Escolha o alcance da operação")
  alcance:str = int(input("Escolha:\n1-UNICAST\n2-BROADCAST\n3-MULTICAST"))

  #Comando a depender da operação
  commandoOperacao:str = ''

  if operacao == 1:
    commandoOperacao += "SELECT * "
    if alcance == 1:
      porta:int = int(input("Digite a porta"))
      commandoOperacao += " FROM data;"
      middleware_connection(porta, commandoOperacao, alcance)
  elif operacao == 2:
    command += "INSERT "
  elif operacao == 3:
    command += "UPDATE "
  middleware_connection(3306, alcance, command)
  middleware_connection(3307)
  middleware_connection(3308)
  # send a thank you message to the client. encoding to send byte type. 
  #c.send('Thank you for connecting'.encode()) 

  # Close the connection with the client 
  #c.close()
  
  # Breaking once connection closed
  #break