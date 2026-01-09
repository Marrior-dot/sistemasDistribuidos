# first of all import the socket library 
#from middleware import middleware_connection
from node import Node
from addr import listaNos
import asyncio
# next create a socket object 
async def main():
  node_id = 1
  socketHost = 'localhost'
  socketPort = 3001
  dbHost ='localhost'  #'172.18.0.1'
  dbPort = 3307
  maquina_2 = Node(node_id,socketHost,socketPort,dbHost,dbPort, listaNos)
  print ("Socket successfully created")
  await maquina_2.requisicoesServidor(listaNos)
if __name__ == "__main__":
    asyncio.run(main())