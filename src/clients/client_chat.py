
import tkinter as tk
from tkinter import scrolledtext, simpledialog
import socket
import threading

class ChatClient:
    def __init__(self, master):
        self.master = master
        master.title("Cliente Chat del Banco")

        self.client_id = simpledialog.askstring("ID de Cliente", "Por favor, ingrese su ID de cliente:", parent=master)
        if not self.client_id:
            master.destroy()
            return
        master.title(f"Cliente Chat del Banco - {self.client_id}")

        self.sock = None
        self.host = "localhost"
        self.port = 8080

        self.chat_area = scrolledtext.ScrolledText(master, wrap=tk.WORD, state='disabled')
        self.chat_area.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)

        self.msg_entry = tk.Entry(master, font=("Helvetica", 12))
        self.msg_entry.pack(padx=10, pady=5, fill=tk.X, expand=False)
        self.msg_entry.bind("<Return>", self.send_message)

        self.send_button = tk.Button(master, text="Enviar Consulta", command=self.send_message)
        self.send_button.pack(padx=10, pady=5)

        self.connect_to_server()

    def connect_to_server(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            self.add_message_to_chat(f"SYSTEM: Conectado al servidor en {self.host}:{self.port}")
            # Iniciar un hilo para escuchar respuestas del servidor
            threading.Thread(target=self.receive_messages, daemon=True).start()
        except Exception as e:
            self.add_message_to_chat(f"SYSTEM: Error al conectar - {e}")

    def add_message_to_chat(self, message):
        self.chat_area.config(state='normal')
        self.chat_area.insert(tk.END, message + '\n')
        self.chat_area.config(state='disabled')
        self.chat_area.yview(tk.END)

    def send_message(self, event=None):
        message_text = self.msg_entry.get()
        if not message_text or not self.sock:
            return

        # Convertir "COMANDO param1 param2" a "COMANDO|param1|param2"
        parts = message_text.split()
        query = '|'.join(parts)
        request = f"QUERY|{self.client_id}|{query}"

        try:
            self.sock.sendall(request.encode('utf-8') + b'\n')
            self.add_message_to_chat(f"Yo: {message_text}")
            self.msg_entry.delete(0, tk.END)
        except Exception as e:
            self.add_message_to_chat(f"SYSTEM: Error al enviar - {e}")

    def receive_messages(self):
        while True:
            try:
                response = self.sock.recv(4096).decode('utf-8').strip()
                if not response:
                    self.add_message_to_chat("SYSTEM: Conexión cerrada por el servidor.")
                    break
                
                # Formato: RESPONSE|ESTADO|MENSAJE
                parts = response.split('|', 2)
                if len(parts) == 3:
                    status = parts[1]
                    message = parts[2].replace('#', '\n - ')
                    self.add_message_to_chat(f"ChatBot ({status}): {message}")
                else:
                    self.add_message_to_chat(f"ChatBot: {response}")

            except Exception as e:
                self.add_message_to_chat(f"SYSTEM: Error de recepción - {e}")
                break
        self.sock.close()

def main():
    root = tk.Tk()
    app = ChatClient(root)
    root.geometry("500x400")
    root.mainloop()

if __name__ == "__main__":
    main()
