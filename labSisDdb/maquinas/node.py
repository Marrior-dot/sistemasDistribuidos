import socket
import json
import asyncio
import psutil
import mysql.connector
from mysql.connector import Error
import time
import re

class Node:
    def __init__(self, node_id: int,socketHost:str,socketPort:int,dbHost:str,dbPort:int, listaNos:list):
        self._node_id = node_id #id
        self._is_coordinator = False #é coordenador ou não
        self.socketHost = socketHost #host do socket
        self.socketPort = socketPort #porta do socket
        self.dbHost = dbHost #host do banco de dados
        self.dbPort = dbPort #porta do banco de dados
        self.listaNos = listaNos #lista com todos os nós 
        self.actual_coordinator = {} #dados sobre o atual coordenador
        #-----------Servidor-------------

        #Verifica se o nó atual pode fazer ou não a eleição
       
    #retorna atual id
    def get_node_id(self) -> int:
        return self._node_id
    #retona porta do nó
    def get_socket_port(self):
        return self.socketPort
    #retorna host do nó
    def get_socket_host(self):
        return self.socketHost
    #retorna host do banco de dados
    def get_db_host(self):
        return self.dbHost
    #retorna porta do banco de dados
    def get_db_port(self):
        return self.dbPort
    #retorna a lista dos nós
    def get_outrasMaquinas(self):
        return self.listaNos
    #retorna se é coordenador ou não
    def is_coordinator(self) -> bool:
        return self._is_coordinator
    #configura como atual coordenador
    def set_actual_coordinator(self, actual_coordinator):
        self.actual_coordinator = actual_coordinator

    #inicia a eleição
    async def iniciarEleicao(self,listaNos:list):
        #Valor começa com o nó que faz a eleição
        maior = {"id":self.get_node_id(),
                 "host":self.get_socket_host(),
                 "port":self.get_socket_port()}
        print("iniciada eleição")
        for no in listaNos:
            #Nó com maior id se torna coordenador
            if(no["id"] > maior["id"]):
                maior = no
        await self.becomeCoordinator(maior,listaNos)

    #Elege o coordenador
    async def becomeCoordinator(self, maior, listaNos:list):
        print(f"Nó de ID {maior["id"]} se torna coordernador")
        #copia da lista de nós
        listaNosCopia = listaNos
        #mensagem enviada para informar os outros nós sobre o atual coordenador
        msg_coordinator = {'id':maior['id'],'host':maior["host"],'port': maior["port"]}
        #print(msg_coordinator)
        #configura no nó atual o coordenador atual
        self.set_actual_coordinator(msg_coordinator)

        #verifica se o nó atual é ou não o atual coordenador
        if(self.get_socket_host() == maior['host'] and self.get_socket_port == maior['port']):
            self.set_coordinator(True)
        
        #adiciona id à mensagem
        #remove o nó que faz a eleição da lista copiada
        listaNosCopia.remove(listaNosCopia[0])
        msg_bytes = json.dumps(msg_coordinator).encode('utf-8')
        for no in listaNosCopia:            
            #inicia o socket
            socket_coordinator = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            #usado para compensar um delay do windows
            socket_coordinator.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            #faz conexão com os outros sockets
            socket_coordinator.connect((no["host"], no["port"]))
            try:
                #print(msg_bytes)
                #garante o envio de todos os bytes
                socket_coordinator.sendall(msg_bytes)
                #permite que a conexão seja fechada apenas se a mensagem tiver sido enviada
                socket_coordinator.shutdown(socket.SHUT_WR)
                print('socket em espera')
                #time.sleep(10)
            except Exception as e:
                print(e)
                socket_coordinator.close()
                continue            
            finally:
                #fecha o socket
                socket_coordinator.close()
                #tempo para os outros nós processarem as mensagens
                time.sleep(2)
                continue

            #envia a mensagem e fecha em seguida
    async def requisicoesServidor(self, listaNos:list):
        self.sSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #Cria o socket
        self.sSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #evita que ocorra o erro de Adress already in use
        self.sSocket.bind((self.socketHost,self.socketPort)) #faz o bind com o host e a porta
        print ("socket binded to %s" %(self.socketPort))
        #Lista com todos os nós disponíveis
        self.sSocket.listen(5) #escuta requisições
        self.sSocket.setblocking(False) #configuração para socket assíncrono
        #self.sSocket.settimeout(10) #time-out do socket, utilizado para dar continuidade ao loop e fazer a eleição novamente
        while True: 
            print ("socket is listening")
            #loop para iniciar funções assíncronas
            loop = asyncio._get_running_loop()
            #copia da lista, possui somente os nós que estão com as portas abertas
            nova_listaNos:list = []
            for no in listaNos:
                #verifica se cada nó possui ou não a porta aberta
                #se sim, é adicionado à nova_lista
                if self.verificar_porta_listening(no['host'],no['port']) == True:
                    nova_listaNos.append(no)

            #O primeiro nó da lista é o responsável por eleger o coordenador
            
            if self.get_socket_host() == nova_listaNos[0]['host'] and self.get_socket_port() == nova_listaNos[0]['port']:
                #Realiza utilizado para a primeira eleição
                coordenador_vazio: bool = True if self.actual_coordinator == {} else False
                #Realiza outra eleição apenas se o nó que é o atual coordenador não estiver mais atuante
                nao_possui_coordernador:bool = self.actual_coordinator != {} and self.verificar_porta_listening(self.actual_coordinator['host'], self.actual_coordinator['port'])
                #Verifica se existe algum nó com id maior para ser o coordenador
                no_id_maior:bool = False 
                #print(self.actual_coordinator)
                if self.actual_coordinator != {}:
                    for no in nova_listaNos:
                        if no['id'] > self.actual_coordinator['id']:
                            no_id_maior = True
                            break
                    
                if coordenador_vazio or nao_possui_coordernador or no_id_maior: 
                    await self.iniciarEleicao(nova_listaNos)
            try:
                #Espera assíncrona de conexões
                c, addr = await asyncio.wait_for(loop.sock_accept(self.sSocket), timeout=10.0)
                c.setblocking(False)
                print(c,addr)
                #processamento da mensagem

                mensagem = await self.processar_mensagem(c,loop)
                copia_mensagem = mensagem
                #Regex para verificar se a mensagem veio pelo coordenador ou não 
                regex_coordenador = r'false'

                #print(mensagem)
                if len(addr) > 0: #Verifica se a conexão possui conteúdo
                    #Se for coordenador manda para os outros nós
                    if self._is_coordinator == True:
                        #identifica se a mensagem é json ou não
                        mensagem_json = self.identify_json_message(c,mensagem)
                        #Se sim, o loop vai para a próxima instância 
                        if mensagem_json == True:
                            continue
                        #print("problema aq")
                        #Conexão com o banco
                        if re.match(regex_coordenador, mensagem):
                            #re.sub(padrão, substituto, string_original)
                            query = re.sub(regex_coordenador, '', mensagem)
                            # Opcional: remover espaços extras que possam sobrar
                            query = query.strip()
                            print(f"Mensagem processada: {query}")
                            self.conexao_db(query)
                        #Mandando para outras máquinas
                        query = f"true\n{query}"
                        for maquina in nova_listaNos:
                            socket_maquina = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            socket_maquina.connect((maquina['host'], maquina['port']))
                            socket_maquina.send(query.encode())
                            socket_maquina.shutdown(socket.SHUT_WR)
                            socket_maquina.close()
                    else:
                        mensagem_json = self.identify_json_message(c,mensagem)
                        if mensagem_json == True:
                            continue 
                        #Se verdadeiro manda para o coordenador
                        print(copia_mensagem)
                        if re.match(regex_coordenador, mensagem):
                            try:
                                socket_maquina = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                socket_maquina.connect((self.actual_coordinator['host'], self.actual_coordinator['port']))
                                socket_maquina.send(mensagem.encode())
                                socket_maquina.shutdown(socket.SHUT_WR)
                                socket_maquina.close()
                                #c.connect((self.actual_coordinator['host'], self.actual_coordinator['port']))
                                #c.send(mensagem.encode('utf-8'))
                                #socket_maquina.shutdown(socket.SHUT_WR)
                                #c.close()
                            except Exception as e:
                                print(f"Erro ao processar: {e}")
                        else:
                            query_db = re.sub(r'true','',mensagem)
                            query_db = query_db.strip()
                            self.conexao_db(query_db)
                        
            except socket.timeout:
                continue
            #await asyncio.sleep(5)

    #Verifica se a mensagem é um json que possui as informações sobre o coordenador
    def identify_json_message(self,c,mensagem:str):
        try:#Verifica se a mensagem recebida indica novo coordenador
            #decodificação em json
            mensagem_json = json.loads(mensagem) 
            print(f'Nó de ID {mensagem_json['id']} é o coordenador atual')
            #Determina o atual coordenador
            self.set_actual_coordinator(mensagem_json)
            #Verifica se o nó que tá executando a função é ou não o coordenador
            if mensagem_json['host'] == self.get_socket_host() and mensagem_json['port'] == self.get_socket_port():
                self.set_coordinator(True)
            c.close()
            return True
        except (ValueError, TypeError) as e:
            print(e)
            c.close()
            return False
        
    def set_coordinator(self, coordinator: bool) -> None:
        self._is_coordinator = coordinator
    
    def conexao_db(self, mensagem):
        #pass
        connection = mysql.connector.connect(
        host=self.dbHost,       # Change to your host (e.g., an IP address)
        port=self.dbPort,
        database='teste',     # The name of your database
        user='root',            # Your MySQL username
        password='teste'  # Your MySQL password
        )
        try:    
            cursor = connection.cursor()
            #Execução da mensagem no banco de dados
            cursor.execute(mensagem)
        except Error as e:
            print(f"Error ao conectar com Mysql: {e}")
        finally:
            #fechar a conexão com o banco
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("Connection closed")

    #Processa a mensagem
    async def processar_mensagem(self,c, loop):
    # Transforma o recv comum em uma tarefa aguardável (awaitable)
        try:
            #Espera pela mensagem do cliente
            data = await asyncio.wait_for(loop.sock_recv(c, 1024), timeout=5.0)
            #se não houver dados é uma conexão de teste apenas
            if not data:
                print("conexão teste detectada")
                c.close()
                return None
            #Decodificação da mensagem em texto pleno
            mensagem = data.decode().strip()
            return mensagem
        except asyncio.TimeoutError:
            print("Conexão mas nenhum dado enviado (timeout)")
            return None
        except Exception as e:
            print(f"Erro na leitura:{e}")
            return None

    def verificar_porta_listening(self, host, porta):
        actual_host = '127.0.0.1' if host == 'localhost' else host

        # 1. VERIFICAÇÃO LOCAL (Usando psutil para processos no mesmo PC)
        # Útil para saber se o processo está rodando nesta máquina
        try:
            conexoes = psutil.net_connections()
            for conn in conexoes:
                # Verifica se o IP bate (ou se está ouvindo em '0.0.0.0' que significa todas as interfaces)
                ip_bate = (conn.laddr.ip == actual_host or conn.laddr.ip == '0.0.0.0')
                if ip_bate and conn.laddr.port == porta and conn.status == 'LISTEN':
                    print(f"Porta {porta} encontrada localmente via psutil.")
                    return True
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            pass # Algumas conexões exigem permissão de admin

        # 2. VERIFICAÇÃO REMOTA (Usando Socket para outros PCs da rede)
        # Se o psutil não achou, tentamos um "aperto de mão" TCP
        try:
            # Tenta criar uma conexão rápida
            with socket.create_connection((actual_host, porta), timeout=0.5):
                print(f"Porta {porta} em {actual_host} está acessível (Remota/LAN).")
                return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False