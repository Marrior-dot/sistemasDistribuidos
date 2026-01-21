import os
import socket
import json
import asyncio
import psutil
import mysql.connector
from mysql.connector import Error
import time
import hashlib
import threading

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

        self.listaNosAtivos = []
        self.thread_running = False

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

    # --- Threads de Monitoramento (Heartbeat + Consistência) ---
    def thread_monitoramento_geral(self):
        """
        Thread única que gerencia Heartbeat e Verificação de Consistência
        """
        print("--- Thread de Monitoramento Iniciada ---")
        ultimo_check_consistencia = time.time()
        INTERVALO_CONSISTENCIA = 10 # Executa checksum a cada 10 segundos

        while self.thread_running:
            temp_ativos = []
            for no in self.listaNos:
                if self.verificar_porta_listening(no['host'], no['port']):
                    temp_ativos.append(no)
            self.listaNosAtivos = temp_ativos
            
            # Verificação de Consistência periódica (apenas coordenador)
            if self._is_coordinator and (time.time() - ultimo_check_consistencia > INTERVALO_CONSISTENCIA):
                self.disparar_verificacao_consistencia()
                ultimo_check_consistencia = time.time()
            
            time.sleep(3) 

    def disparar_verificacao_consistencia(self):
        checksums_local = self.obter_checksum_por_tabela()
        print(f"[AUDITORIA] Checksums: {checksums_local}")
        
        # Envia o dicionário JSON para os workers
        pacote = self.empacotar_mensagem("CONSISTENCY_CHECK", json.dumps(checksums_local))
        threading.Thread(target=self.broadcast_simples, args=(pacote,)).start()

    def broadcast_simples(self, pacote):
        # Envia para todos os nós ativos sem esperar resposta
        for maquina in self.listaNosAtivos:
            if maquina['id'] == self._node_id: continue
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                s.connect((maquina['host'], maquina['port']))
                s.sendall(pacote)
                try: s.shutdown(socket.SHUT_WR)
                except: pass
                s.close()
            except: pass

    def verificar_porta_listening(self, host, porta):
        actual_host = '127.0.0.1' if host == 'localhost' else host
        try:
            with socket.create_connection((actual_host, porta), timeout=0.5): 
                return True
        except: 
            return False

    def obter_checksum_por_tabela(self):
        """
        Retorna um dicionário com o checksum de cada tabela.
        Ex: {'clientes': 998877, 'pedidos': 112233}
        """
        conn = None
        checksums = {}
        try:
            conn = mysql.connector.connect(host=self.dbHost, port=self.dbPort, database='teste', user='root', password='teste')
            cursor = conn.cursor()
            
            cursor.execute("SHOW TABLES")
            tabelas = cursor.fetchall()
            
            for tab in tabelas:
                nome_tabela = tab[0]
                cursor.execute(f"CHECKSUM TABLE {nome_tabela}")
                res = cursor.fetchone()
                if res:
                    checksums[nome_tabela] = res[1] # Nome: Valor
                    
        except Error as e:
            print(f"Erro Checksum DB: {e}")
        finally:
            if conn and conn.is_connected(): conn.close()
            
        return checksums

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
                                
                                if coord_data['id'] == self._node_id:
                                    print("A rede diz que EU sou o coordenador.")
                                    self.set_coordinator(True)
                                    self.ja_sincronizou = True                                
                                encontrou = True
                                break
                except: pass
                finally: s.close()
        return encontrou

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
        
        # Envia a notícia para todos
        listaNosCopia = listaNos.copy()
        for no in listaNosCopia:
            if no['id'] == self._node_id: continue
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            try:
                s.connect((no["host"], no["port"]))
                s.sendall(msg_bytes)
                s.shutdown(socket.SHUT_WR)
            except: pass
            finally:
                s.close()

    async def realizar_bootstrap_sync(self):
        if self._is_coordinator:
            self.ja_sincronizou = True
            return

        if self.actual_coordinator and self.actual_coordinator['id'] == self._node_id:
            self.set_coordinator(True)
            self.ja_sincronizou = True
            return

        while self.actual_coordinator and not self._is_coordinator and not self.ja_sincronizou:
            print(f"--- [BOOTSTRAP] Tentando sincronizar com ID {self.actual_coordinator['id']} ---")
            
            if not self.verificar_porta_listening(self.actual_coordinator['host'], self.actual_coordinator['port']):
                print("[BOOTSTRAP] Coordenador morreu durante sync.")
                self.actual_coordinator = {} 
                break

            sucesso = await self.solicitar_sincronizacao()
            if sucesso:
                print("[BOOTSTRAP] Sincronização concluída!")
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
        
        await self.descobrir_coordenador(listaNos)
        await self.realizar_bootstrap_sync()

        self.thread_running = True
        t_monitor = threading.Thread(target=self.thread_monitoramento_geral)
        t_monitor.daemon = True 
        t_monitor.start()
        
        await asyncio.sleep(0.5)

        while True: 
            loop = asyncio._get_running_loop()
            
            if not self.listaNosAtivos:
                lista_para_eleicao = [n for n in listaNos if n['id'] == self._node_id]
            else:
                lista_para_eleicao = self.listaNosAtivos[:] # Cópia
            
            meu_no = next((n for n in listaNos if n['id'] == self._node_id), None)
            if meu_no and meu_no not in lista_para_eleicao:
                lista_para_eleicao.append(meu_no)
            lista_para_eleicao.sort(key=lambda x: x['id'])

            # Lógica de Eleição
            if self.get_socket_host() == lista_para_eleicao[0]['host'] and self.get_socket_port() == lista_para_eleicao[0]['port']:
                coordenador_vazio = (self.actual_coordinator == {})
                coord_esta_ativo = False
                if self.actual_coordinator:
                    for n in lista_para_eleicao:
                        if n['id'] == self.actual_coordinator['id']:
                            coord_esta_ativo = True
                            break
                
                coord_morto = (self.actual_coordinator != {}) and (not coord_esta_ativo)
                
                no_id_maior = False 
                if self.actual_coordinator != {}:
                    for no in lista_para_eleicao:
                        if no['id'] > self.actual_coordinator['id']:
                            no_id_maior = True
                            break
                
                if coordenador_vazio or coord_morto or no_id_maior: 
                    await self.iniciarEleicao(lista_para_eleicao)

            try:
                c, addr = await asyncio.wait_for(loop.sock_accept(self.sSocket), timeout=1.0)
                c.setblocking(False)
                dados_brutos = await self.receber_dados(c, loop) 
                
                if dados_brutos:
                    try: mensagem = json.loads(dados_brutos)
                    except: c.close(); continue

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
                            print(f"--> [COORD] Executando: {conteudo}")
                            resultado_db = self.conexao_db(conteudo)

                            if not conteudo.strip().upper().startswith("SELECT"):
                                novo_id = self.registrar_log(conteudo)
                                self.salvar_estado(novo_id)
                                # REPLICAR PARA TODOS
                                await self.replicar_para_workers(conteudo, self.listaNos) 

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
                        self.registrar_log_externo({'id': self.ultimo_id_processado, 'query': conteudo})
                    
                    elif tipo == 'SYNC_REQUEST':
                        # Sync Incremental (Log)
                        ultimo_id_cliente = int(conteudo)
                        print(f"Pedido de Sync Incremental (ID: {ultimo_id_cliente})")
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
                    
                    elif tipo == 'CONSISTENCY_CHECK':
                        checksums_mestre = json.loads(conteudo) # Dicionário do Mestre
                        checksums_local = self.obter_checksum_por_tabela() # Dicionário Local
                        
                        tabelas_divergentes = []

                        # Compara cada tabela do mestre com a local
                        for tabela, hash_mestre in checksums_mestre.items():
                            hash_local = checksums_local.get(tabela)
                            
                            if hash_local != hash_mestre:
                                print(f"Divergência na tabela '{tabela}' (Mestre:{hash_mestre} != Local:{hash_local})")
                                tabelas_divergentes.append(tabela)
                        
                        # Se encontrou diferenças, pede snapshot PARCIAL
                        if tabelas_divergentes:
                            print(f"Solicitando correção para: {tabelas_divergentes}")
                            await self.solicitar_snapshot(tabelas_divergentes)
                        else:
                            print("[AUDITORIA] Réplica já está sincronizada.")
                            pass

                    elif tipo == 'REQUEST_SNAPSHOT':
                        # O conteudo agora é uma lista de tabelas ex: ["usuarios", "pedidos"]
                        # Se vier vazio, manda tudo (fallback)
                        tabelas_solicitadas = json.loads(conteudo) if conteudo else []
                        
                        print(f"Gerando Snapshot parcial para: {tabelas_solicitadas if tabelas_solicitadas else 'TODAS'}")
                        
                        dump_parcial = {}
                        conn = mysql.connector.connect(host=self.dbHost, port=self.dbPort, database='teste', user='root', password='teste')
                        try:
                            cursor = conn.cursor()
                            
                            # Se não pediu nenhuma específica, pega todas
                            if not tabelas_solicitadas:
                                cursor.execute("SHOW TABLES")
                                tabelas_solicitadas = [t[0] for t in cursor.fetchall()]
                            
                            for nome_tabela in tabelas_solicitadas:
                                # Verifica se tabela existe
                                try:
                                    cursor.execute(f"SELECT * FROM {nome_tabela}")
                                    linhas = cursor.fetchall()
                                    dump_parcial[nome_tabela] = linhas
                                except: pass
                                
                        except Error as e:
                            print(f"Erro Dump: {e}")
                        finally:
                            conn.close()

                        pacote_resp = self.empacotar_mensagem("SNAPSHOT_DATA", json.dumps(dump_parcial, default=str))
                        await loop.sock_sendall(c, pacote_resp)

                    elif tipo == 'SNAPSHOT_DATA':
                         print("Aplicando Snapshot de recuperação...")
                         dados_recuperados = json.loads(conteudo)
                         
                         # ATENÇÃO: TRUNCATE APAGA TUDO PARA REESCREVER
                         self.conexao_db("TRUNCATE TABLE data")
                         
                         if dados_recuperados:
                             for linha in dados_recuperados:
                                 # Adapte este INSERT para suas colunas reais
                                 # Exemplo genérico: INSERT INTO data VALUES ('val1', 'val2')
                                 vals = "', '".join([str(v) for v in linha])
                                 query = f"INSERT INTO data VALUES ('{vals}')"
                                 self.conexao_db(query)
                         print("Recuperação completa. Banco sincronizado.")

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
    
    def gerar_checksum(self, conteudo: str) -> str:
        return hashlib.md5(conteudo.encode('utf-8')).hexdigest()

    def empacotar_mensagem(self, tipo: str, conteudo) -> bytes:
        if isinstance(conteudo, (dict, list)): conteudo_str = json.dumps(conteudo, default=str)
        else: conteudo_str = str(conteudo)
        dados = {"tipo": tipo, "conteudo": conteudo_str, "checksum": self.gerar_checksum(conteudo_str)}
        print(f"Nó de ID {self._node_id} envia: {dados}")
        return json.dumps(dados, default=str).encode('utf-8')

    def validar_integridade(self, dados_json: dict) -> bool:
        return dados_json.get('checksum') == self.gerar_checksum(str(dados_json.get('conteudo')))
    
    async def receber_dados(self, c, loop):
        try:
            data = await asyncio.wait_for(loop.sock_recv(c, 8192), timeout=5.0)
            return data.decode().strip()
        except: return None

    async def replicar_para_workers(self, query, lista_completa_nos):
        print(f"--- Iniciando Replicação para {len(lista_completa_nos)-1} nós ---")
        pacote = self.empacotar_mensagem("REPLICATION", query)
        for maquina in lista_completa_nos:
            if maquina['id'] == self._node_id: continue
            print(f"Tentando replicar para Nó {maquina['id']} ({maquina['host']}:{maquina['port']})...")
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3) 
                s.connect((maquina['host'], maquina['port']))
                s.sendall(pacote)
                try: s.shutdown(socket.SHUT_WR)
                except: pass
                s.close()
                print(f"Replicação enviada para Nó {maquina['id']}")
            except Exception as e:
                print(f"Falha ao replicar para Nó {maquina['id']}: {e}")

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

    async def solicitar_snapshot(self, tabelas_alvo=None):
        if not self.actual_coordinator: return
        
        # Envia a lista de tabelas que deve corrigir
        conteudo_req = json.dumps(tabelas_alvo) if tabelas_alvo else ""
        pacote = self.empacotar_mensagem("REQUEST_SNAPSHOT", conteudo_req)
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); s.settimeout(30.0)
        try:
            s.connect((self.actual_coordinator['host'], self.actual_coordinator['port']))
            s.sendall(pacote)
            
            dados_completos = b""
            while True:
                chunk = s.recv(4096)
                if not chunk: break
                dados_completos += chunk
            
            if dados_completos:
                msg = json.loads(dados_completos.decode())
                if msg['tipo'] == 'SNAPSHOT_DATA':
                     dados_recuperados = json.loads(msg['conteudo'])
                     
                     conn = mysql.connector.connect(host=self.dbHost, port=self.dbPort, database='teste', user='root', password='teste')
                     cursor = conn.cursor()
                     try:
                         cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                         for nome_tabela, linhas in dados_recuperados.items():
                             print(f"--- Reparando tabela: {nome_tabela} ---")
                             cursor.execute(f"TRUNCATE TABLE {nome_tabela}")
                             if linhas:
                                 for linha in linhas:
                                     vals = "', '".join([str(v).replace("'", "''") for v in linha])
                                     query = f"INSERT INTO {nome_tabela} VALUES ('{vals}')"
                                     cursor.execute(query)
                         conn.commit()
                         print("Tabelas divergentes sincronizadas.")
                     except Exception as e:
                         print(f"Erro restore: {e}")
                     finally:
                         cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
                         conn.close()
        except Exception as e:
            print(f"Erro Snapshot: {e}")
        finally: s.close()