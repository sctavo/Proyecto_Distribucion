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

# --- INICIO: Importaciones para la BD ---
import sqlite3
# --- FIN: Importaciones para la BD ---


# Constantes
HOST = '0.0.0.0'  # Escuchar en todas las interfaces
PORT = 65432        # Puerto para la Matriz
COMBUSTIBLES = ["93", "95", "97", "Diesel", "Kerosene"] # Tipos válidos

class MatrizServer:
    def __init__(self, host, port, log_callback):
        self.host = host
        self.port = port
        self.log_callback = log_callback # Función para enviar logs a la GUI
        self.server_socket = None
        self.distribuidores = []
        self.lock = threading.Lock()
        
        # --- Base de Datos Central ---
        self.db_path = "matriz/db_matriz.sqlite" 
        self._init_db() 

    def log(self, message):
        """Envía un mensaje de log a la GUI (o a la consola si no hay GUI)."""
        now = datetime.now().strftime("%H:%M:%S")
        if self.log_callback:
            # Aseguramos que la llamada se haga en el hilo principal de Tkinter
            self.log_callback(f"[{now}] {message}")
        else:
            print(f"[{now}] {message}")

    # --- INICIO: Funciones de Base de Datos ---
    def _init_db(self):
        """Inicializa la BD central y crea la tabla si no existe."""
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS transacciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                distribuidor_id TEXT NOT NULL,
                surtidor_id TEXT NOT NULL,
                combustible TEXT NOT NULL,
                litros REAL NOT NULL,
                cargas INTEGER NOT NULL
            )
            """)
            conn.commit()
            conn.close()
            self.log(f"Base de datos central inicializada en: {self.db_path}")
        except Exception as e:
            self.log(f"Error inicializando la base de datos: {e}")

    def _save_transaction(self, msg: TransaccionReportMessage):
        """Guarda un reporte de transacción en la base de datos central."""
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            
            sql = """
            INSERT INTO transacciones 
                (timestamp, distribuidor_id, surtidor_id, combustible, litros, cargas) 
            VALUES (?, ?, ?, ?, ?, ?)
            """
            params = (datetime.now(), msg.distribuidor_id, msg.surtidor_id, msg.combustible, msg.litros, msg.cargas)
            cursor.execute(sql, params)
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.log(f"Error guardando transacción en BD central: {e}")
    
    def fetch_reports(self):
        """Consulta la BD y retorna los datos para los reportes."""
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            
            # Reporte 1: Litros por combustible
            sql_comb = """
            SELECT 
                combustible, 
                SUM(litros) as total_litros,
                SUM(cargas) as total_cargas
            FROM transacciones
            GROUP BY combustible
            ORDER BY total_litros DESC
            """
            cursor.execute(sql_comb)
            report_comb = cursor.fetchall()
            
            # Reporte 2: Actividad por distribuidor
            sql_dist = """
            SELECT
                distribuidor_id,
                SUM(litros) as total_litros,
                SUM(cargas) as total_cargas
            FROM transacciones
            GROUP BY distribuidor_id
            ORDER BY distribuidor_id ASC
            """
            cursor.execute(sql_dist)
            report_dist = cursor.fetchall()
            
            conn.close()
            return report_comb, report_dist
            
        except Exception as e:
            self.log(f"Error generando reportes: {e}")
            return [], []
            
    # --- FIN: Funciones de Base de Datos ---

    def start(self):
        """Inicia el servidor y escucha conexiones."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        self.log(f"🏠 Servidor Matriz escuchando en {self.host}:{self.port}")

        try:
            while True:
                client_socket, addr = self.server_socket.accept()
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
                    self.log(f"🔌 Distribuidor {addr} desconectado.")
                    break
                
                msg_obj = deserialize(msg_bytes)

                if isinstance(msg_obj, TransaccionReportMessage):
                    log_msg = (f"📈 Reporte de '{msg_obj.distribuidor_id}' ({addr}): "
                               f"Surtidor {msg_obj.surtidor_id}, "
                               f"{msg_obj.combustible}, {msg_obj.litros:.2f}L, {msg_obj.cargas} cargas")
                    self.log(log_msg)
                    
                    self._save_transaction(msg_obj) 
                          
                elif isinstance(msg_obj, HeartbeatMessage):
                    self.log(f"❤️ Heartbeat de {msg_obj.id} ({addr}): {msg_obj.estado}")
                    
                else:
                    self.log(f"🤔 Mensaje desconocido de {addr}: {msg_obj}")

        except ConnectionError as e:
            self.log(f"❌ Error de conexión con {addr}: {e}")
        finally:
            with self.lock:
                self.distribuidores.remove(client_socket)
            client_socket.close()

    def broadcast_price(self, combustible, precio_base):
        """Envía una actualización de precio a TODOS los distribuidores."""
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
                    self.log(f"Error enviando a {sock.getpeername()}: {e}")
                    disconnected_clients.append(sock)

            for sock in disconnected_clients:
                self.distribuidores.remove(sock)
                sock.close()
        
        self.log(f"✅ Precio enviado a {len(self.distribuidores)} distribuidores.")

# --- INICIO: Clase para la GUI (AdminApp) ---

class AdminApp:
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("Admin Matriz (Nivel 3)")
        self.root.geometry("700x550") # Tamaño inicial (un poco más grande)
        
        self.style = ttk.Style()
        self.style.theme_use('clam') 
        
        # --- 1. Crear el Notebook (Pestañas) ---
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # --- 2. Crear los frames para las pestañas ---
        self.control_tab = ttk.Frame(self.notebook, padding="10")
        self.reports_tab = ttk.Frame(self.notebook, padding="10")
        
        self.notebook.add(self.control_tab, text='Control y Logs')
        self.notebook.add(self.reports_tab, text='Reportes')
        
        # --- 3. Poblar la pestaña de "Control y Logs" ---
        self.setup_control_tab()
        
        # --- 4. Poblar la pestaña de "Reportes" ---
        self.setup_reports_tab()

        self.server = None
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def setup_control_tab(self):
        """Pone todos los widgets en la pestaña de Control."""
        # --- Frame de Controles (Arriba) ---
        controls_frame = ttk.Labelframe(self.control_tab, text="Control de Precios", padding="10")
        controls_frame.pack(fill=tk.X, expand=False, pady=5)
        
        controls_frame.columnconfigure(1, weight=1)
        
        ttk.Label(controls_frame, text="Combustible:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.comb_var = tk.StringVar()
        self.comb_dropdown = ttk.Combobox(
            controls_frame, textvariable=self.comb_var, values=COMBUSTIBLES, state="readonly"
        )
        self.comb_dropdown.grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        
        ttk.Label(controls_frame, text="Precio Base:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.precio_entry = ttk.Entry(controls_frame)
        self.precio_entry.grid(row=1, column=1, padx=5, pady=5, sticky=tk.EW)

        self.send_button = ttk.Button(
            controls_frame, text="Transmitir Precio", command=self.on_send_price
        )
        self.send_button.grid(row=0, column=2, rowspan=2, padx=10, pady=5, sticky="NS")

        # --- Frame de Logs (Abajo) ---
        logs_frame = ttk.Labelframe(self.control_tab, text="Logs del Servidor", padding="10")
        logs_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.log_text = scrolledtext.ScrolledText(
            logs_frame, wrap=tk.WORD, state='disabled', height=15
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def setup_reports_tab(self):
        """Pone todos los widgets en la pestaña de Reportes."""
        
        # Botón de Actualizar
        self.refresh_button = ttk.Button(
            self.reports_tab, text="Actualizar Reportes", command=self.on_refresh_reports
        )
        self.refresh_button.pack(pady=10)
        
        # --- Frame para las dos tablas ---
        tables_frame = ttk.Frame(self.reports_tab)
        tables_frame.pack(fill=tk.BOTH, expand=True)
        tables_frame.columnconfigure(0, weight=1)
        tables_frame.columnconfigure(1, weight=1)
        tables_frame.rowconfigure(1, weight=1)
        
        # --- Tabla 1: Reporte por Combustible ---
        ttk.Label(tables_frame, text="Totales por Combustible", font=("-weight bold")).grid(row=0, column=0, padx=10, pady=(0,5))
        
        self.report_comb_tree = ttk.Treeview(
            tables_frame, columns=("combustible", "litros", "cargas"), show="headings"
        )
        self.report_comb_tree.heading("combustible", text="Combustible")
        self.report_comb_tree.heading("litros", text="Total Litros")
        self.report_comb_tree.heading("cargas", text="Total Cargas")
        self.report_comb_tree.grid(row=1, column=0, padx=10, pady=5, sticky="nsew")

        # --- Tabla 2: Reporte por Distribuidor ---
        ttk.Label(tables_frame, text="Totales por Distribuidor", font=("-weight bold")).grid(row=0, column=1, padx=10, pady=(0,5))
        
        self.report_dist_tree = ttk.Treeview(
            tables_frame, columns=("distribuidor", "litros", "cargas"), show="headings"
        )
        self.report_dist_tree.heading("distribuidor", text="Distribuidor ID")
        self.report_dist_tree.heading("litros", text="Total Litros")
        self.report_dist_tree.heading("cargas", text="Total Cargas")
        self.report_dist_tree.grid(row=1, column=1, padx=10, pady=5, sticky="nsew")

    def on_closing(self):
        """Maneja el evento de cierre de la ventana."""
        if messagebox.askokcancel("Salir", "¿Seguro que quieres cerrar el servidor de Matriz?"):
            self.root.destroy()
            if self.server and self.server.server_socket:
                self.server.server_socket.close()
            print("Cerrando GUI y servidor...")

    def on_send_price(self):
        """Callback del botón 'Transmitir Precio'."""
        comb = self.comb_var.get()
        precio_str = self.precio_entry.get()
        
        if not comb:
            messagebox.showerror("Error", "Debe seleccionar un tipo de combustible.")
            return
            
        try:
            precio_int = int(precio_str)
            if precio_int <= 0: raise ValueError
        except ValueError:
            messagebox.showerror("Error", "El precio debe ser un número entero positivo.")
            return

        if self.server:
            self.server.broadcast_price(comb, precio_int)
            self.precio_entry.delete(0, tk.END)
        else:
            messagebox.showerror("Error", "El servidor no está conectado.")
            
    def on_refresh_reports(self):
        """Callback del botón 'Actualizar Reportes'."""
        if not self.server:
            messagebox.showerror("Error", "El servidor no está conectado.")
            return
        
        # 1. Pedir los datos al servidor
        report_comb, report_dist = self.server.fetch_reports()
        
        # 2. Limpiar tablas (Treeviews)
        for item in self.report_comb_tree.get_children():
            self.report_comb_tree.delete(item)
        for item in self.report_dist_tree.get_children():
            self.report_dist_tree.delete(item)
            
        # 3. Insertar datos en tabla de combustibles
        for row in report_comb:
            # Formatear los litros a 2 decimales
            formatted_row = (row[0], f"{row[1]:.2f}", row[2])
            self.report_comb_tree.insert("", tk.END, values=formatted_row)
            
        # 4. Insertar datos en tabla de distribuidores
        for row in report_dist:
            formatted_row = (row[0], f"{row[1]:.2f}", row[2])
            self.report_dist_tree.insert("", tk.END, values=formatted_row)
            
        self.log_to_widget("Reportes actualizados desde la base de datos.")

    def log_to_widget(self, message):
        """Función thread-safe para añadir logs al widget de texto."""
        try:
            self.root.after(0, self._append_log, message)
        except Exception:
            pass

    def _append_log(self, message):
        """Función auxiliar que se ejecuta en el hilo de la GUI."""
        try:
            self.log_text.config(state='normal')
            self.log_text.insert(tk.END, message + "\n")
            self.log_text.config(state='disabled')
            self.log_text.see(tk.END)
        except Exception:
            pass

# --- FIN: Clase para la GUI (AdminApp) ---


# --- Punto de entrada del script (MODIFICADO) ---
if __name__ == "__main__":
    
    root = tk.Tk()
    app = AdminApp(root)
    
    server = MatrizServer(HOST, PORT, app.log_to_widget)
    app.server = server

    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()

    root.mainloop()

    print("Cerrando programa...")