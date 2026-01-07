# first of all import the socket library 
#from middleware import middleware_connection
from node import Node
# next create a socket object 
def main():
  node_id = 0
  socketHost = 'localhost'
  socketPort = 3001
  dbHost = 'localhost'
  dbPort = 3307
  outrasMaquinas:list = [
     {"host":'localhost', 
      "port":3000},
      {"host":'localhost', 
      "port":3002}]
  maquina_2 = Node(node_id,socketHost,socketPort,dbHost,dbPort)
  print ("Socket successfully created")
if __name__ == "__main__":
    main()