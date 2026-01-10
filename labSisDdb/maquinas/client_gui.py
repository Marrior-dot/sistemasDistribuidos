import tkinter as tk
from tkinter import messagebox, scrolledtext
import socket
import json
import hashlib
import threading

class ClientGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Cliente DDB - Middleware MySQL")
        self.root.geometry("600x500")

        # --- Configurações de Conexão ---
        frame_config = tk.LabelFrame(root, text="Configuração do Nó Alvo", padx=10, pady=10)
        frame_config.pack(fill="x", padx=10, pady=5)

        tk.Label(frame_config, text="Host:").grid(row=0, column=0, padx=5)
        self.entry_host = tk.Entry(frame_config, width=15)
        self.entry_host.insert(0, "localhost")
        self.entry_host.grid(row=0, column=1, padx=5)

        tk.Label(frame_config, text="Porta:").grid(row=0, column=2, padx=5)
        self.entry_port = tk.Entry(frame_config, width=8)
        self.entry_port.insert(0, "3000")
        self.entry_port.grid(row=0, column=3, padx=5)

        # --- Área de Query ---
        frame_query = tk.LabelFrame(root, text="Comando SQL", padx=10, pady=10)
        frame_query.pack(fill="both", expand=True, padx=10, pady=5)

        self.text_query = scrolledtext.ScrolledText(frame_query, height=5, font=("Consolas", 10))
        self.text_query.pack(fill="both", expand=True)
        self.text_query.insert("1.0", "SELECT * FROM teste.data") # Exemplo padrão

        # --- Botões ---
        frame_btn = tk.Frame(root, pady=5)
        frame_btn.pack(fill="x", padx=10)

        self.btn_enviar = tk.Button(frame_btn, text="Executar Query", command=self.iniciar_envio, bg="#4CAF50", fg="white", font=("Arial", 10, "bold"))
        self.btn_enviar.pack(side="right", padx=5)
        
        self.btn_limpar = tk.Button(frame_btn, text="Limpar Log", command=self.limpar_log)
        self.btn_limpar.pack(side="left", padx=5)

        # --- Área de Resultados ---
        frame_result = tk.LabelFrame(root, text="Resultado / Log", padx=10, pady=10)
        frame_result.pack(fill="both", expand=True, padx=10, pady=5)

        self.text_result = scrolledtext.ScrolledText(frame_result, height=10, state='disabled', font=("Consolas", 9))
        self.text_result.pack(fill="both", expand=True)

    # --- Lógica do Protocolo (Mesma do Node) ---
    def gerar_checksum(self, conteudo: str) -> str:
        return hashlib.md5(conteudo.encode('utf-8')).hexdigest()

    def log_msg(self, msg):
        self.text_result.config(state='normal')
        self.text_result.insert(tk.END, msg + "\n")
        self.text_result.see(tk.END)
        self.text_result.config(state='disabled')

    def limpar_log(self):
        self.text_result.config(state='normal')
        self.text_result.delete('1.0', tk.END)
        self.text_result.config(state='disabled')

    def iniciar_envio(self):
        # Roda em thread separada para não travar a interface
        threading.Thread(target=self.enviar_query).start()

    def enviar_query(self):
        host = self.entry_host.get()
        try:
            port = int(self.entry_port.get())
        except ValueError:
            messagebox.showerror("Erro", "Porta deve ser um número.")
            return

        query = self.text_query.get("1.0", tk.END).strip()
        if not query:
            messagebox.showwarning("Aviso", "Digite uma query.")
            return

        self.log_msg("-" * 40)
        self.log_msg(f"Enviando para {host}:{port}...")

        # Monta pacote
        dados = {
            "tipo": "CLIENT_REQUEST",
            "conteudo": query,
            "checksum": self.gerar_checksum(query)
        }
        
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10.0)
            s.connect((host, port))
            
            msg_bytes = json.dumps(dados).encode('utf-8')
            s.sendall(msg_bytes)

            # Recebe resposta (Loop de leitura para garantir JSON completo)
            resposta_bytes = b""
            while True:
                chunk = s.recv(4096)
                if not chunk: break
                resposta_bytes += chunk
            
            s.close()

            if resposta_bytes:
                resp_json = json.loads(resposta_bytes.decode())
                
                # Validação de integridade
                checksum_server = resp_json.get('checksum')
                conteudo_server = resp_json.get('conteudo') # Isso vem como string JSON
                
                # Calcula checksum do que chegou (conteudo_server é a string interna)
                if checksum_server == self.gerar_checksum(conteudo_server):
                    # Decodifica o conteúdo interno (Resultado + ID)
                    dados_finais = json.loads(conteudo_server)
                    
                    executor = dados_finais.get('executor_id')
                    resultado = dados_finais.get('resultado')
                    
                    self.log_msg(f"✅ SUCESSO [Executado pelo Nó {executor}]")
                    
                    # Formatação bonita se for lista (SELECT)
                    if isinstance(resultado, list):
                        for linha in resultado:
                            self.log_msg(str(linha))
                    else:
                        self.log_msg(str(resultado))
                else:
                    self.log_msg("❌ ERRO: Checksum da resposta inválido!")
            else:
                self.log_msg("⚠️ Sem resposta do servidor.")

        except ConnectionRefusedError:
            self.log_msg("❌ Erro: Não foi possível conectar ao nó.")
        except Exception as e:
            self.log_msg(f"❌ Erro: {str(e)}")

if __name__ == "__main__":
    root = tk.Tk()
    app = ClientGUI(root)
    root.mainloop()