import socket
import json
import hashlib
import ast # Para converter a string de volta para lista/tupla

TARGET_HOST = 'localhost'
TARGET_PORT = 3000 # Tente variar (3001, 3002) para testar o redirecionamento

def gerar_checksum(conteudo):
    conteudo_str = str(conteudo)
    return hashlib.md5(conteudo_str.encode('utf-8')).hexdigest()

def enviar_query(query):
    dados = {
        "tipo": "CLIENT_REQUEST",
        "conteudo": query,
        "checksum": gerar_checksum(query)
    }
    mensagem_json = json.dumps(dados)
    
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((TARGET_HOST, TARGET_PORT))
        
        # 1. Envia
        client.sendall(mensagem_json.encode('utf-8'))
        
        # 2. RECEBE A RESPOSTA (O pulo do gato)
        resposta_bytes = client.recv(4096)
        
        if resposta_bytes:
            resposta_json = json.loads(resposta_bytes.decode())
            
            # Valida integridade da resposta
            checksum_recebido = resposta_json['checksum']
            conteudo_resposta = resposta_json['conteudo']
            
            if checksum_recebido == gerar_checksum(conteudo_resposta):
                print("\n--- RESPOSTA DO SERVIDOR ---")
                
                try:
                    # 1. Decodifica o pacote de resposta (JSON com resultado + ID)
                    dados_resposta = json.loads(conteudo_resposta)
                    
                    resultado = dados_resposta.get('resultado')
                    executor_id = dados_resposta.get('executor_id')
                    
                    print(f"[Executado pelo Nó ID: {executor_id}]")
                    
                    # 2. Formata o resultado (se for lista de tuplas do MySQL)
                    if isinstance(resultado, list):
                        for linha in resultado:
                            print(linha)
                    else:
                        print(resultado)
                        
                except json.JSONDecodeError:
                    # Caso receba algo fora do padrão (compatibilidade)
                    print(conteudo_resposta)
                except Exception as e:
                    print(f"Erro ao processar resposta: {e}")
                    
                print("----------------------------")
            else:
                print("[ALERTA] Resposta corrompida (Checksum inválido)!")

        client.close()
        
    except ConnectionRefusedError:
        print(f"[ERRO] Não conectou em {TARGET_PORT}.")
    except Exception as e:
        print(f"[ERRO] {e}")

if __name__ == "__main__":
    print("--- Cliente DDB ---")
    while True:
        query = input("\nSQL >> ")
        if query.lower() == 'sair': break
        enviar_query(query)