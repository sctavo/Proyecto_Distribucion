# servidor central para distribuidores
import socket
import threading
import sys
import os

# --- INICIO: Hack para importar 'common' ---
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.append(project_root)

from common.framer import frame_message, receive_message
from common.messages import (
    serialize, deserialize, PrecioUpdateMessage, 
    TransaccionReportMessage, HeartbeatMessage
)
# --- FIN: Hack para importar 'common' ---

# --- INICIO: Importaciones para la GUI ---
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from datetime import datetime
# --- FIN: Importaciones para la GUI ---


# Constantes
HOST = '127.0.0.1'  # IP local para pruebas
PORT = 65432        # Puerto para la Matriz
COMBUSTIBLES = ["93", "95", "97", "Diesel", "Kerosene"] # Tipos válidos

class MatrizServer:
    # --- MODIFICADO: Añadido 'log_callback' ---
    def __init__(self, host, port, log_callback):
        self.host = host
        self.port = port
        self.log_callback = log_callback # Función para enviar logs a la GUI
        self.server_socket = None
        self.distribuidores = []
        self.lock = threading.Lock()

    def log(self, message):
        """Envía un mensaje de log a la GUI (o a la consola si no hay GUI)."""
        now = datetime.now().strftime("%H:%M:%S")
        if self.log_callback:
            self.log_callback(f"[{now}] {message}")
        else:
            print(f"[{now}] {message}")

    def start(self):
        """Inicia el servidor y escucha conexiones."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        # --- MODIFICADO: Usar self.log ---
        self.log(f"🏠 Servidor Matriz escuchando en {self.host}:{self.port}")

        try:
            while True:
                client_socket, addr = self.server_socket.accept()
                # --- MODIFICADO: Usar self.log ---
                self.log(f"📦 Nueva conexión de Distribuidor desde {addr}")

                with self.lock:
                    self.distribuidores.append(client_socket)

                client_thread = threading.Thread(
                    target=self.handle_distribuidor, 
                    args=(client_socket, addr)
                )
                client_thread.daemon = True
                client_thread.start()
                
        except Exception:
            self.log("Cerrando servidor Matriz...")
        finally:
            self.server_socket.close()

    def handle_distribuidor(self, client_socket, addr):
        """Maneja la comunicación entrante de un solo distribuidor."""
        try:
            while True:
                msg_bytes = receive_message(client_socket)
                if msg_bytes is None:
                    # --- MODIFICADO: Usar self.log ---
                    self.log(f"🔌 Distribuidor {addr} desconectado.")
                    break
                
                msg_obj = deserialize(msg_bytes)

                if isinstance(msg_obj, TransaccionReportMessage):
                    # TODO: Aquí guardaríamos en la BD Central
                    # --- MODIFICADO: Usar self.log ---
                    log_msg = (f"📈 Reporte de '{msg_obj.distribuidor_id}' ({addr}): "
                               f"Surtidor {msg_obj.surtidor_id}, "
                               f"{msg_obj.combustible}, {msg_obj.litros:.2f}L, {msg_obj.cargas} cargas")
                    self.log(log_msg)
                          
                elif isinstance(msg_obj, HeartbeatMessage):
                    # --- MODIFICADO: Usar self.log ---
                    self.log(f"❤️ Heartbeat de {msg_obj.id} ({addr}): {msg_obj.estado}")
                    
                else:
                    # --- MODIFICADO: Usar self.log ---
                    self.log(f"🤔 Mensaje desconocido de {addr}: {msg_obj}")

        except ConnectionError as e:
            # --- MODIFICADO: Usar self.log ---
            self.log(f"❌ Error de conexión con {addr}: {e}")
        finally:
            with self.lock:
                self.distribuidores.remove(client_socket)
            client_socket.close()
            # self.log(f"Cerrada conexión con {addr}") # Log opcional

    def broadcast_price(self, combustible, precio_base):
        """Envía una actualización de precio a TODOS los distribuidores."""
        # --- MODIFICADO: Usar self.log ---
        self.log(f"📣 Transmitiendo nuevo precio: {combustible} a ${precio_base}")
        
        msg_obj = PrecioUpdateMessage(combustible, precio_base)
        msg_bytes = serialize(msg_obj)
        framed_msg = frame_message(msg_bytes)
        
        disconnected_clients = []
        with self.lock:
            for sock in self.distribuidores:
                try:
                    sock.sendall(framed_msg)
                except Exception as e:
                    # --- MODIFICADO: Usar self.log ---
                    self.log(f"Error enviando a {sock.getpeername()}: {e}")
                    disconnected_clients.append(sock)

            for sock in disconnected_clients:
                self.distribuidores.remove(sock)
                sock.close()
        
        # --- MODIFICADO: Usar self.log ---
        self.log(f"✅ Precio enviado a {len(self.distribuidores)} distribuidores.")

    # --- ELIMINADO: run_admin_cli() ---
    # Esta función ha sido reemplazada por la clase AdminApp


# --- INICIO: Nueva Clase para la GUI ---

class AdminApp:
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("Admin Matriz (Nivel 3)")
        self.root.geometry("600x450") # Tamaño inicial
        
        # Aplicar un estilo moderno
        self.style = ttk.Style()
        self.style.theme_use('clam') # 'clam', 'alt', 'default', 'vista'
        
        # --- Configurar el contenedor principal ---
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # --- 1. Frame de Controles (Arriba) ---
        controls_frame = ttk.Labelframe(main_frame, text="Control de Precios", padding="10")
        controls_frame.pack(fill=tk.X, expand=False, pady=5)
        
        # Layout de 2x3
        controls_frame.columnconfigure(1, weight=1) # Columna de entry/combo se expande
        
        # Fila 1: Combustible
        ttk.Label(controls_frame, text="Combustible:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.comb_var = tk.StringVar()
        self.comb_dropdown = ttk.Combobox(
            controls_frame, 
            textvariable=self.comb_var, 
            values=COMBUSTIBLES,
            state="readonly" # Evita que el usuario escriba
        )
        self.comb_dropdown.grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        
        # Fila 2: Precio
        ttk.Label(controls_frame, text="Precio Base:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.precio_entry = ttk.Entry(controls_frame)
        self.precio_entry.grid(row=1, column=1, padx=5, pady=5, sticky=tk.EW)

        # Fila 2 (col 2): Botón
        self.send_button = ttk.Button(
            controls_frame, 
            text="Transmitir Precio", 
            command=self.on_send_price
        )
        self.send_button.grid(row=0, column=2, rowspan=2, padx=10, pady=5, sticky="NS")

        # --- 2. Frame de Logs (Abajo) ---
        logs_frame = ttk.Labelframe(main_frame, text="Logs del Servidor", padding="10")
        logs_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.log_text = scrolledtext.ScrolledText(
            logs_frame, 
            wrap=tk.WORD, 
            state='disabled', # Empieza como solo lectura
            height=15
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # --- Referencia al Servidor (se seteará después) ---
        self.server = None

    def on_send_price(self):
        """Callback del botón 'Transmitir Precio'."""
        comb = self.comb_var.get()
        precio_str = self.precio_entry.get()
        
        # --- Validación ---
        if not comb:
            messagebox.showerror("Error", "Debe seleccionar un tipo de combustible.")
            return
            
        try:
            precio_int = int(precio_str)
            if precio_int <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("Error", "El precio debe ser un número entero positivo.")
            return

        # Si todo está OK, llamar al broadcast del servidor
        if self.server:
            # El broadcast ya se encarga de loguear
            self.server.broadcast_price(comb, precio_int)
            
            # Limpiar el campo de precio
            self.precio_entry.delete(0, tk.END)
        else:
            messagebox.showerror("Error", "El servidor no está conectado.")
            
    def log_to_widget(self, message):
        """Función thread-safe para añadir logs al widget de texto."""
        try:
            # Habilitar para escribir
            self.log_text.config(state='normal')
            self.log_text.insert(tk.END, message + "\n")
            # Deshabilitar de nuevo
            self.log_text.config(state='disabled')
            # Auto-scroll al final
            self.log_text.see(tk.END)
        except Exception as e:
            print(f"Error al loguear en GUI: {e}")

# --- FIN: Nueva Clase para la GUI ---


# --- Punto de entrada del script (MODIFICADO) ---
if __name__ == "__main__":
    
    # 1. Crear la ventana principal de la GUI
    root = tk.Tk()
    
    # 2. Crear la aplicación (la GUI)
    app = AdminApp(root)

    # 3. Crear la instancia del servidor, pasándole la función de log de la GUI
    server = MatrizServer(HOST, PORT, app.log_to_widget)

    # 4. Darle a la GUI una referencia al servidor (para el botón de "Transmitir")
    app.server = server

    # 5. Inicia el servidor (aceptar conexiones) en un hilo separado
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()

    # 6. Inicia el bucle principal de la GUI (esto reemplaza a run_admin_cli())
    root.mainloop()

    print("Cerrando programa...")