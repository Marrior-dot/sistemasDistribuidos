# first of all import the socket library 
#from middleware import middleware_connection
from node import Node
from addr import listaNos
import asyncio
# next create a socket object 
async def main():
  node_id = 0
  socketHost = 'localhost'
  socketPort = 3000     
  dbHost = 'localhost'
  dbPort = 3306
  maquina_1 = Node(node_id,socketHost,socketPort,dbHost,dbPort, listaNos)
  print ("Socket successfully created")
  await maquina_1.requisicoesServidor(listaNos)
if __name__ == "__main__":
    asyncio.run(main())