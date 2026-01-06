import socket
class Node:
    def __init__(self, node_id: int,socketHost:str,socketPort:int,dbHost:str,dbPort:int):
        self._node_id = node_id
        self._is_coordinator = False
        #-----------Servidor-------------
        self.sSocket = socket.socket()
        self.sSocket.bind((socketHost,socketPort))
        print ("socket binded to %s" %(socketPort))
        self.sSocket.listen(5)
        print ("socket is listening")
        #-----------Servidor-------------

        #-----------Cliente Banco de dados--------------
        self.sDb = socket.socket()
        self.sDb.connect((dbHost, dbPort))

        #-----------Requisições Servidor-------------
        while True:
            c, addr = self.sSocket.accept()

    def get_node_id(self) -> int:
        return self._node_id

    def is_coordinator(self) -> bool:
        return self._is_coordinator

    def set_coordinator(self, coordinator: bool) -> None:
        self._is_coordinator = coordinator

   