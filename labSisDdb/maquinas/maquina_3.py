# first of all import the socket library 
#from middleware import middleware_connection
from node import Node
from addr import listaNos
import asyncio
# next create a socket object 
async def main():
  node_id = 2
  socketHost = 'localhost'
  socketPort = 3002
  dbHost = 'localhost'
  dbPort = 3308
  maquina_3 = Node(node_id,socketHost,socketPort,dbHost,dbPort, listaNos)
  print ("Socket successfully created")
  await maquina_3.requisicoesServidor(listaNos)
if __name__ == "__main__":
    asyncio.run(main())