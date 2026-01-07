import socket
class Node:
    def __init__(self, node_id: int,socketHost:str,socketPort:int,dbHost:str,dbPort:int, outrasMaquinas:list):
        self._node_id = node_id
        self._is_coordinator = False
        #-----------Servidor-------------
        self.sSocket = socket.socket() #Cria o socket
        self.sSocket.bind((socketHost,socketPort)) #faz o bind com o host e a porta
        print ("socket binded to %s" %(socketPort))
        self.sSocket.listen(5) #escuta requisições
        print ("socket is listening")
        #-----------Servidor-------------
        #-----------Cliente--------------
        #self.sDb = socket.socket() #Socket do banco de dados
        #self.sDb.connect((dbHost, dbPort))

        #-----------Requisições Servidor-------------
        while True:
            c, addr = self.sSocket.accept()
            print(c.recv(1024).decode())
            if len(addr) > 0:
                self.sDb = socket.socket() #Socket do banco de dados
                self.sDb.connect((dbHost, dbPort))
                self.sDb.send(c.recv(1024).decode())
                if self._is_coordinator == True:
                    for maquina in outrasMaquinas:
                        pass #Conectar a outras máquinas e mandar o mesmo comando, mandar em broadcast

    def get_node_id(self) -> int:
        return self._node_id

    def is_coordinator(self) -> bool:
        return self._is_coordinator

    def set_coordinator(self, coordinator: bool) -> None:
        self._is_coordinator = coordinator

   