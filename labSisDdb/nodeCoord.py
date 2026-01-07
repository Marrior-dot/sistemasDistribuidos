import socket
class NodeCoord:
    def __init__(self, node_id: int,socketHost:str,socketPort:int):
        self.socketHost = socketHost
        self.socketPort = socketPort
        self._node_id = node_id
        self.coordenador = False
        self.socket = socket.socket()
    def get_socket(self):
        return self.socket 

    def get_node_id(self) -> int:
        return self._node_id

    def get_socket_host(self) -> str:
        return self.socketHost
    
    def get_socket_port(self) -> str:
        return self.socketPort

    def is_coordinator(self) -> bool:
        return self._is_coordinator

    def set_coordinator(self, coordinator: bool) -> None:
        self._is_coordinator = coordinator

    def iniciarEleicao(self,listaNos:list):
        maior:NodeCoord = self
        for no in listaNos:
            if(no.get_node_id() > maior.get_node_id()):
                maior = no
        self.becomeCoordinator(maior)

    def receiveElectionMessage(self, sender):
        if(self.get_node_id() > sender.get_node_id()):
            sender.receiveResponde(self)
    
    def receiveResponse(self,sender):
        print(f"Nó {self.get_node_id()} recebe resposta do nó {sender.get_node_id()}")
    
    def becomeCoordinator(self, sender):
        print(f"Nó {sender.get_node_id()} se torna coordernador")
        sender.set_coordinator(True)


   