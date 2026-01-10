import socket
import json
import asyncio
import psutil
import mysql.connector
from mysql.connector import Error
import time
import re
import hashlib


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
        print(f"Nó de ID {maior['id']} se torna coordernador")
        
        #Define quem é o novo chefe nos dados
        msg_coordinator = {'id':maior['id'], 'host':maior["host"], 'port': maior["port"]}
        self.set_actual_coordinator(msg_coordinator)

        #Verifica se EU sou o maior. 
        if (self.get_socket_host() == maior['host'] and self.get_socket_port() == maior['port']):
            self.set_coordinator(True)
        else:
            #SE O NOVO CHEFE NÃO SOU EU, EU DEIXO DE SER COORDENADOR
            self.set_coordinator(False) 
        

        #Prepara mensagem (Protocolo Novo)
        msg_bytes = self.empacotar_mensagem("ELECTION_WINNER", msg_coordinator)
        
        listaNosCopia = listaNos.copy()
        
        for no in listaNosCopia:
            if no['id'] == self._node_id: 
                continue

            socket_coordinator = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket_coordinator.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            try:
                socket_coordinator.connect((no["host"], no["port"]))
                socket_coordinator.sendall(msg_bytes)
                socket_coordinator.shutdown(socket.SHUT_WR)
            except Exception as e:
                print(e)
            finally:
                socket_coordinator.close()
                time.sleep(0.5)

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
                nao_possui_coordernador:bool = self.actual_coordinator != {} and not self.verificar_porta_listening(self.actual_coordinator['host'], self.actual_coordinator['port'])
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
                #Espera conexão
                c, addr = await asyncio.wait_for(loop.sock_accept(self.sSocket), timeout=10.0)
                c.setblocking(False)
                
                #Lê os dados
                dados_brutos = await self.receber_dados(c, loop) 
                
                if dados_brutos:
                    try:
                        mensagem = json.loads(dados_brutos)
                    except json.JSONDecodeError:
                        c.close()
                        continue

                    #Valida Checksum
                    if not self.validar_integridade(mensagem):
                        print("Erro de integridade.")
                        c.close()
                        continue

                    tipo = mensagem.get('tipo')
                    conteudo = mensagem.get('conteudo')

                    resposta_para_cliente = None

                    if tipo == 'CLIENT_REQUEST':
                        print(f"Executando query: {conteudo}")
                        
                        resposta_payload = ""

                        if self._is_coordinator:
                            print(f"--> [COORD] Executando localmente: {conteudo}")
                            resultado_db = self.conexao_db(conteudo)
                            
                            #2. Replica (apenas se não for SELECT)
                            if not conteudo.strip().upper().startswith("SELECT"):
                                await self.replicar_para_workers(conteudo, nova_listaNos)
                            
                            #3. Monta o pacote de resposta com ID
                            dados_retorno = {
                                "resultado": resultado_db if resultado_db else "Operação realizada com sucesso.",
                                "executor_id": self.get_node_id()
                            }
                            #Converte para string JSON para trafegar seguro
                            resposta_payload = json.dumps(dados_retorno)
                            
                        else:
                            print(f"--> [WORKER] Redirecionando para o coordenador: {conteudo}")
                            resposta_payload = await self.consultar_coordenador(conteudo)

                        #O conteudo agora é uma string JSON contendo {"resultado":..., "executor_id":...}
                        pacote_resposta = self.empacotar_mensagem("RESPONSE", resposta_payload)
                        
                        await loop.sock_sendall(c, pacote_resposta)

                    elif tipo == 'REPLICATION':
                        self.conexao_db(conteudo)

                    elif tipo == 'ELECTION_WINNER':
                         self.set_actual_coordinator(conteudo)
                         
                         #Se o ID recebido for o meu, viro True. Se não, viro False.
                         if conteudo['id'] == self.get_node_id():
                             self.set_coordinator(True)
                         else:
                             self.set_coordinator(False) 
                         
                         print(f"Novo coordenador definido: ID {conteudo['id']}")

                c.close()

            except socket.timeout:
                continue
            except Exception as e:
                print(f"Erro: {e}")
                c.close()

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
        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.dbHost,
                port=self.dbPort,
                database='teste',
                user='root',
                password='teste'
            )
            
            cursor = connection.cursor()
            cursor.execute(mensagem)
            
            #Verifica se a query retornou linhas (Ex: SELECT)
            if cursor.with_rows:
                resultado = cursor.fetchall()
                print(f"--- [DB] Resultado da Query: {resultado} ---")
                return resultado
            else:
                #Se for INSERT/UPDATE/DELETE, confirma a transação
                connection.commit()
                print("--- [DB] Alteração commitada com sucesso ---")
                return None

        except Error as e:
            print(f"Erro ao conectar com Mysql: {e}")
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()

    #Processa a mensagem
    async def processar_mensagem(self,c, loop):
    #Transforma o recv comum em uma tarefa aguardável (awaitable)
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

        #1. VERIFICAÇÃO LOCAL (Usando psutil para processos no mesmo PC)
        #Útil para saber se o processo está rodando nesta máquina
        try:
            conexoes = psutil.net_connections()
            for conn in conexoes:
                #Verifica se o IP bate (ou se está ouvindo em '0.0.0.0' que significa todas as interfaces)
                ip_bate = (conn.laddr.ip == actual_host or conn.laddr.ip == '0.0.0.0')
                if ip_bate and conn.laddr.port == porta and conn.status == 'LISTEN':
                    print(f"Porta {porta} encontrada localmente via psutil.")
                    return True
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            pass #Algumas conexões exigem permissão de admin

        #2. VERIFICAÇÃO REMOTA (Usando Socket para outros PCs da rede)
        #Se o psutil não achou, tentamos um "aperto de mão" TCP
        try:
            #Tenta criar uma conexão rápida
            with socket.create_connection((actual_host, porta), timeout=0.5):
                print(f"Porta {porta} em {actual_host} está acessível (Remota/LAN).")
                return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False
    
    #Gera o checksum MD5 do conteúdo da mensagem
    def gerar_checksum(self, conteudo: str) -> str:
        return hashlib.md5(conteudo.encode('utf-8')).hexdigest()

    #Cria o pacote JSON padrão
    def empacotar_mensagem(self, tipo: str, conteudo: str) -> bytes:
        dados = {
            "tipo": tipo,    
            "conteudo": conteudo,
            "checksum": self.gerar_checksum(str(conteudo))
        }
        return json.dumps(dados).encode('utf-8')

    #Valida se a mensagem recebida não foi corrompida
    def validar_integridade(self, dados_json: dict) -> bool:
        conteudo = str(dados_json.get('conteudo'))
        checksum_recebido = dados_json.get('checksum')
        checksum_calculado = self.gerar_checksum(conteudo)
        return checksum_recebido == checksum_calculado
    
    #Método auxiliar para ler do socket (antigo processar_mensagem simplificado)
    async def receber_dados(self, c, loop):
        try:
            data = await asyncio.wait_for(loop.sock_recv(c, 4096), timeout=5.0)
            return data.decode().strip()
        except:
            return None

    #Envia para todos os outros nós (Broadcast de replicação)
    async def replicar_para_workers(self, query, lista_atual_nos):
        pacote = self.empacotar_mensagem("REPLICATION", query)
        
        for maquina in lista_atual_nos:
            #Não mandar para si mesmo
            if maquina['id'] == self._node_id:
                continue

            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                s.connect((maquina['host'], maquina['port']))
                s.sendall(pacote)
                s.close()
            except Exception as e:
                print(f"Falha ao replicar para nó {maquina['id']}: {e}")

    #Envia para o coordenador (Unicast)
    async def repassar_para_coordenador(self, query):
        if not self.actual_coordinator:
            print("Erro: Sem coordenador para repassar.")
            return

        pacote = self.empacotar_mensagem("CLIENT_REQUEST", query)
        
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.actual_coordinator['host'], self.actual_coordinator['port']))
            s.sendall(pacote)
            s.close()
        except Exception as e:
            print(f"Erro ao repassar ao coordenador: {e}")

    
    #Worker envia para coordenador e AGUARDA resposta
    async def consultar_coordenador(self, query):
        if not self.actual_coordinator:
            return "Erro: Sem coordenador disponível."

        pacote = self.empacotar_mensagem("CLIENT_REQUEST", query)
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0) #Timeout para não travar se o coord morrer
        try:
            s.connect((self.actual_coordinator['host'], self.actual_coordinator['port']))
            s.sendall(pacote)
            
            #Espera a resposta do coordenador
            dados_retorno = s.recv(4096) 
            if dados_retorno:
                msg_json = json.loads(dados_retorno.decode())
                #Retorna apenas o conteudo (o resultado do banco)
                return msg_json.get('conteudo')
            else:
                return "Sem resposta do coordenador."
        except Exception as e:
            return f"Erro na comunicação com coordenador: {e}"
        finally:
            s.close()