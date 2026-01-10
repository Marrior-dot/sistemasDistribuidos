import os
import socket
import json
import asyncio
import psutil
import mysql.connector
from mysql.connector import Error
import time
import hashlib

class Node:
    def __init__(self, node_id: int, socketHost:str, socketPort:int, dbHost:str, dbPort:int, listaNos:list):
        self._node_id = node_id
        self._is_coordinator = False
        self.socketHost = socketHost
        self.socketPort = socketPort
        self.dbHost = dbHost
        self.dbPort = dbPort
        self.listaNos = listaNos
        self.actual_coordinator = {}
        
        self.arquivo_estado = f"estado_node_{node_id}.json"
        self.arquivo_log = f"log_operacoes_{node_id}.json"
        
        self.log_operacoes = self.carregar_log_disco()
        self.ultimo_id_processado = self.carregar_estado()
        self.ja_sincronizou = False

    # --- Persistência ---
    def carregar_log_disco(self):
        if os.path.exists(self.arquivo_log):
            try:
                with open(self.arquivo_log, 'r') as f:
                    return json.load(f)
            except: return []
        return []

    def salvar_log_disco(self):
        try:
            with open(self.arquivo_log, 'w') as f:
                json.dump(self.log_operacoes, f, default=str)
        except: pass

    def registrar_log(self, query):
        novo_id = len(self.log_operacoes) + 1
        item = {'id': novo_id, 'query': query}
        self.log_operacoes.append(item)
        self.salvar_log_disco()
        return novo_id

    def registrar_log_externo(self, item_log):
        ids_existentes = [i['id'] for i in self.log_operacoes]
        if item_log['id'] not in ids_existentes:
            self.log_operacoes.append(item_log)
            # Ordena por ID para garantir consistência
            self.log_operacoes.sort(key=lambda x: x['id'])
            self.salvar_log_disco()

    def carregar_estado(self):
        if os.path.exists(self.arquivo_estado):
            try:
                with open(self.arquivo_estado, 'r') as f:
                    data = json.load(f)
                    return data.get('ultimo_id', 0)
            except: return 0
        return 0

    def salvar_estado(self, novo_id):
        self.ultimo_id_processado = novo_id
        with open(self.arquivo_estado, 'w') as f:
            json.dump({'ultimo_id': novo_id}, f)

    # --- Getters/Setters ---
    def get_node_id(self) -> int: return self._node_id
    def get_socket_port(self): return self.socketPort
    def get_socket_host(self): return self.socketHost
    def get_db_host(self): return self.dbHost
    def get_db_port(self): return self.dbPort
    def is_coordinator(self) -> bool: return self._is_coordinator
    
    def set_actual_coordinator(self, actual_coordinator):
        self.actual_coordinator = actual_coordinator
    
    def set_coordinator(self, coordinator: bool) -> None:
        self._is_coordinator = coordinator

    # --- Descoberta ---
    async def descobrir_coordenador(self, listaNos):
        print("Buscando coordenador na rede...")
        pacote = self.empacotar_mensagem("WHO_IS_COORD", "")
        encontrou = False
        
        for no in listaNos:
            if no['id'] == self._node_id: continue
            if self.verificar_porta_listening(no['host'], no['port']):
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2.0)
                try:
                    s.connect((no['host'], no['port']))
                    s.sendall(pacote)
                    dados = s.recv(4096)
                    if dados:
                        msg = json.loads(dados.decode())
                        if msg['tipo'] == 'COORDINATOR_INFO':
                            coord_data = json.loads(msg['conteudo'])
                            if coord_data and 'id' in coord_data:
                                print(f"Coordenador encontrado: ID {coord_data['id']}")
                                self.set_actual_coordinator(coord_data)
                                encontrou = True
                                break
                except: pass
                finally: s.close()
        return encontrou

    # --- Eleição ---
    async def iniciarEleicao(self, listaNos:list):
        maior = {"id":self.get_node_id(), "host":self.get_socket_host(), "port":self.get_socket_port()}
        print(">>> INICIANDO ELEIÇÃO <<<")
        for no in listaNos:
            if(no["id"] > maior["id"]):
                maior = no
        await self.becomeCoordinator(maior, listaNos)

    async def becomeCoordinator(self, maior, listaNos:list):
        print(f"Vencedor da Eleição: Nó {maior['id']}")
        msg_coordinator = {'id':maior['id'], 'host':maior["host"], 'port': maior["port"]}
        self.set_actual_coordinator(msg_coordinator)

        if (self.get_socket_host() == maior['host'] and self.get_socket_port() == maior['port']):
            self.set_coordinator(True)
            self.ja_sincronizou = True
        else:
            self.set_coordinator(False) 
            self.ja_sincronizou = False
        
        msg_bytes = self.empacotar_mensagem("ELECTION_WINNER", msg_coordinator)
        
        listaNosCopia = listaNos.copy()
        for no in listaNosCopia:
            if no['id'] == self._node_id: continue
            socket_coordinator = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socket_coordinator.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            try:
                socket_coordinator.connect((no["host"], no["port"]))
                socket_coordinator.sendall(msg_bytes)
                socket_coordinator.shutdown(socket.SHUT_WR)
            except: pass
            finally:
                socket_coordinator.close()
                time.sleep(0.1)

    # --- CORE: BARREIRA DE SINCRONIZAÇÃO (NOVO) ---
    async def realizar_bootstrap_sync(self):
        """
        Bloqueia a inicialização até que o nó esteja sincronizado com o coordenador.
        Se o coordenador cair durante o processo, libera para eleição.
        """
        while self.actual_coordinator and not self._is_coordinator and not self.ja_sincronizou:
            print(f"--- [BOOTSTRAP] Tentando sincronizar com ID {self.actual_coordinator['id']} ---")
            
            # Verifica se coordenador ainda está vivo
            if not self.verificar_porta_listening(self.actual_coordinator['host'], self.actual_coordinator['port']):
                print("[BOOTSTRAP] Coordenador morreu durante sync. Abortando sync para iniciar eleição.")
                self.actual_coordinator = {} # Força lógica de eleição no loop principal
                break

            sucesso = await self.solicitar_sincronizacao()
            if sucesso:
                print("[BOOTSTRAP] Sincronização concluída com sucesso!")
                self.ja_sincronizou = True
                break
            else:
                print("[BOOTSTRAP] Falha no sync. Tentando novamente em 3s...")
                await asyncio.sleep(3)

    # --- Loop Principal ---
    async def requisicoesServidor(self, listaNos:list):
        self.sSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sSocket.bind((self.socketHost,self.socketPort))
        print ("Servidor Online na porta %s" %(self.socketPort))
        self.sSocket.listen(5)
        self.sSocket.setblocking(False)
        
        # 1. Fase de Descoberta
        await self.descobrir_coordenador(listaNos)

        # 2. Barreira de Sincronização (Impede eleição enquanto estiver desatualizado)
        await self.realizar_bootstrap_sync()

        # 3. Loop de Operação Normal
        while True: 
            loop = asyncio._get_running_loop()
            
            nova_listaNos:list = []
            for no in listaNos:
                if self.verificar_porta_listening(no['host'],no['port']):
                    nova_listaNos.append(no)

            # Lógica de Eleição (Só ocorre se já passou da barreira de sync)
            if self.get_socket_host() == nova_listaNos[0]['host'] and self.get_socket_port() == nova_listaNos[0]['port']:
                coordenador_vazio = (self.actual_coordinator == {})
                coord_morto = self.actual_coordinator != {} and not self.verificar_porta_listening(self.actual_coordinator['host'], self.actual_coordinator['port'])
                
                # Se eu sou maior que o atual, eu devo assumir, MAS eu já estou sincronizado
                # porque passei pela barreira.
                no_id_maior = False 
                if self.actual_coordinator != {}:
                    for no in nova_listaNos:
                        if no['id'] > self.actual_coordinator['id']:
                            no_id_maior = True
                            break
                
                if coordenador_vazio or coord_morto or no_id_maior: 
                    await self.iniciarEleicao(nova_listaNos)

            try:
                c, addr = await asyncio.wait_for(loop.sock_accept(self.sSocket), timeout=5.0)
                c.setblocking(False)
                dados_brutos = await self.receber_dados(c, loop) 
                
                if dados_brutos:
                    try: mensagem = json.loads(dados_brutos)
                    except: 
                        c.close(); continue

                    if not self.validar_integridade(mensagem):
                        c.close(); continue

                    tipo = mensagem.get('tipo')
                    conteudo = mensagem.get('conteudo')

                    if tipo == 'WHO_IS_COORD':
                        resp = self.actual_coordinator if self.actual_coordinator else {}
                        pacote = self.empacotar_mensagem("COORDINATOR_INFO", resp)
                        await loop.sock_sendall(c, pacote)

                    elif tipo == 'CLIENT_REQUEST':
                        resposta_payload = ""
                        if self._is_coordinator:
                            print(f"--> [COORD] Query: {conteudo}")
                            resultado_db = self.conexao_db(conteudo)
                            if not conteudo.strip().upper().startswith("SELECT"):
                                novo_id = self.registrar_log(conteudo)
                                self.salvar_estado(novo_id)
                                await self.replicar_para_workers(conteudo, nova_listaNos)
                            dados_retorno = {"resultado": resultado_db if resultado_db else "Sucesso.", "executor_id": self.get_node_id()}
                            resposta_payload = json.dumps(dados_retorno, default=str)
                        else:
                            print(f"--> [WORKER] Redirecionando...")
                            resposta_payload = await self.consultar_coordenador(conteudo)
                        pacote_resposta = self.empacotar_mensagem("RESPONSE", resposta_payload)
                        await loop.sock_sendall(c, pacote_resposta)

                    elif tipo == 'REPLICATION':
                        self.conexao_db(conteudo)
                        self.ultimo_id_processado += 1
                        self.salvar_estado(self.ultimo_id_processado)
                        # Salva no log para futuro
                        self.registrar_log_externo({'id': self.ultimo_id_processado, 'query': conteudo})

                    elif tipo == 'SYNC_REQUEST':
                        ultimo_id_cliente = int(conteudo)
                        print(f"Pedido de Sync (ID Cliente: {ultimo_id_cliente})")
                        logs_faltantes = [op for op in self.log_operacoes if op['id'] > ultimo_id_cliente]
                        pacote_resposta = self.empacotar_mensagem("SYNC_RESPONSE", json.dumps(logs_faltantes, default=str))
                        await loop.sock_sendall(c, pacote_resposta)

                    elif tipo == 'ELECTION_WINNER':
                         dados_coord = json.loads(conteudo)
                         self.set_actual_coordinator(dados_coord)
                         if dados_coord['id'] == self.get_node_id():
                             self.set_coordinator(True)
                             self.ja_sincronizou = True
                         else:
                             self.set_coordinator(False) 
                             self.ja_sincronizou = False 
                         print(f"Novo Coordenador: ID {dados_coord['id']}")

                try: c.shutdown(socket.SHUT_WR)
                except: pass
                c.close()

            except socket.timeout: continue
            except Exception as e: 
                if 'c' in locals(): c.close()

    # --- DB, Rede, Utils ---
    def conexao_db(self, mensagem):
        connection = None
        try:
            connection = mysql.connector.connect(host=self.dbHost, port=self.dbPort, database='teste', user='root', password='teste')
            cursor = connection.cursor()
            cursor.execute(mensagem)
            if cursor.with_rows: return cursor.fetchall()
            else:
                connection.commit()
                return None
        except Error as e:
            print(f"Erro BD: {e}")
            return str(e)
        finally:
            if connection and connection.is_connected(): cursor.close(); connection.close()

    def verificar_porta_listening(self, host, porta):
        actual_host = '127.0.0.1' if host == 'localhost' else host
        try:
            for conn in psutil.net_connections():
                if (conn.laddr.ip == actual_host or conn.laddr.ip == '0.0.0.0') and conn.laddr.port == porta and conn.status == 'LISTEN': return True
        except: pass 
        try:
            with socket.create_connection((actual_host, porta), timeout=0.2): return True
        except: return False
            
    def gerar_checksum(self, conteudo: str) -> str:
        return hashlib.md5(conteudo.encode('utf-8')).hexdigest()

    def empacotar_mensagem(self, tipo: str, conteudo) -> bytes:
        if isinstance(conteudo, (dict, list)): conteudo_str = json.dumps(conteudo, default=str)
        else: conteudo_str = str(conteudo)
        dados = {"tipo": tipo, "conteudo": conteudo_str, "checksum": self.gerar_checksum(conteudo_str)}
        return json.dumps(dados, default=str).encode('utf-8')

    def validar_integridade(self, dados_json: dict) -> bool:
        return dados_json.get('checksum') == self.gerar_checksum(str(dados_json.get('conteudo')))
    
    async def receber_dados(self, c, loop):
        try:
            data = await asyncio.wait_for(loop.sock_recv(c, 8192), timeout=5.0)
            return data.decode().strip()
        except: return None

    async def replicar_para_workers(self, query, lista_atual_nos):
        pacote = self.empacotar_mensagem("REPLICATION", query)
        for maquina in lista_atual_nos:
            if maquina['id'] == self._node_id: continue
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2); s.connect((maquina['host'], maquina['port']))
                s.sendall(pacote); s.shutdown(socket.SHUT_WR); s.close()
            except: pass

    async def consultar_coordenador(self, query):
        if not self.actual_coordinator: return "Sem coordenador."
        pacote = self.empacotar_mensagem("CLIENT_REQUEST", query)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); s.settimeout(10.0) 
        try:
            s.connect((self.actual_coordinator['host'], self.actual_coordinator['port']))
            s.sendall(pacote)
            dados = b""
            while True:
                chunk = s.recv(4096)
                if not chunk: break
                dados += chunk
            if dados: return json.loads(dados.decode()).get('conteudo')
            else: return "Sem resposta."
        except Exception as e: return f"Erro: {e}"
        finally: s.close()
    
    async def solicitar_sincronizacao(self):
        if not self.actual_coordinator: return False
        print(f"Sincronizando... (Último ID Local: {self.ultimo_id_processado})")
        pacote = self.empacotar_mensagem("SYNC_REQUEST", str(self.ultimo_id_processado))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); s.settimeout(15.0) 
        try:
            s.connect((self.actual_coordinator['host'], self.actual_coordinator['port']))
            s.sendall(pacote)
            resposta_bytes = b""
            while True:
                try:
                    chunk = s.recv(4096)
                    if not chunk: break 
                    resposta_bytes += chunk
                except socket.timeout:
                    if resposta_bytes: break
                    else: return False
            if resposta_bytes:
                resposta = json.loads(resposta_bytes.decode())
                if self.validar_integridade(resposta):
                    lista_missing = json.loads(resposta['conteudo'])
                    if not lista_missing: print("Nenhum dado novo.")
                    for item in lista_missing:
                        print(f"Aplicando Log ID {item['id']}")
                        self.conexao_db(item['query'])
                        self.salvar_estado(item['id'])
                        self.registrar_log_externo(item)
                    return True 
                else: print("Erro Checksum no Sync.")
            return False
        except Exception as e:
            print(f"Erro Sync: {e}")
            return False
        finally: s.close()