# first of all import the socket library 
#from middleware import middleware_connection
from node import Node
# next create a socket object 
def main():
  node_id = 0
  socketHost = 'localhost'
  socketPort = 3000     
  dbHost = 'localhost'
  dbPort = 3306
  outrasMaquinas:list = [
     {"host":'localhost', 
      "port":3001},
      {"host":'localhost', 
      "port":3002}]
  maquina_1 = Node(node_id,socketHost,socketPort,dbHost,dbPort, outrasMaquinas)
  print ("Socket successfully created")
if __name__ == "__main__":
    main()