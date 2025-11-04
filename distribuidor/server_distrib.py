# servidor para surtidores + cliente hacia matriz
import socket
import threading
import sys
import os
import time
import sqlite3 # para almacenamiento local de transacciones 
from datetime import datetime # para timestamps de sqlite

# --- INICIO: Hack para importar 'common' ---
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.append(project_root)

from common.framer import frame_message, receive_message
from common.messages import (
    serialize, deserialize, 
    PrecioUpdateMessage, PrecioLocalUpdateMessage,
    TransaccionReportMessage, HeartbeatMessage
)
# --- FIN: Hack para importar 'common' ---

# --- Configuración del Distribuidor ---
MATRIZ_HOST = os.environ.get('MATRIZ_HOST', '127.0.0.1')
MATRIZ_PORT = 65432
# Factor de utilidad (ej: 15% de margen)
UTILIDAD_FACTOR = 1.15 
# Tiempo (en segundos) para reintentar la conexión a la Matriz
RECONNECT_DELAY = 5 

class DistribuidorServer:
    def __init__(self, id, host, port):
        self.id = id
        self.host = host  # IP en la que escucha a los Surtidores
        self.port = port  # Puerto en el que escucha a los Surtidores
        
        # ---  Base de datos local --- #
        self.db_path = f"distribuidor/db_local_{self.id}.sqlite"
        self._init_db() # Llama a la función de la base de datos

        # --- Estado del Servidor (Nivel 2) ---
        self.server_socket = None # Socket para escuchar a los Surtidores
        self.surtidores = [] # Lista de sockets de surtidores conectados
        self.lock_surtidores = threading.Lock() # Lock para la lista de surtidores
        
        # --- Caché Local y Lógica de Negocio ---
        self.current_prices = {} # Ej: {'95': 1650, '93': 1600}
        self.lock_prices = threading.Lock()
        
        # --- Estado del Cliente (Nivel 2 -> 3) ---
        self.socket_to_matriz = None # Socket conectado a la Matriz
        self.lock_matriz_socket = threading.Lock()
        self.is_connected_to_matriz = threading.Event() # Flag para saber el estado

    # --- INICIO: Funciones de Base de Datos ---

    def _init_db(self):
        """Inicializa la base de datos SQLite y crea la tabla si no existe."""
        try:
            # os.makedirs(os.path.dirname(self.db_path), exist_ok=True) # Opcional
            
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            
            # Crear tabla de transacciones
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS transacciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                surtidor_id TEXT NOT NULL,
                combustible TEXT NOT NULL,
                litros REAL NOT NULL,
                cargas INTEGER NOT NULL,
                distribuidor_id TEXT NOT NULL,
                sincronizado_matriz INTEGER DEFAULT 0 
            )
            """)
            
            conn.commit()
            conn.close()
            print(f"Base de datos local inicializada en: {self.db_path}")
        except Exception as e:
            print(f"Error inicializando la base de datos: {e}")

    def _save_transaction(self, msg: TransaccionReportMessage) -> int | None:
        """Guarda un reporte de transacción y retorna su ID de BD."""
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            
            sql = """
            INSERT INTO transacciones 
                (timestamp, surtidor_id, combustible, litros, cargas, distribuidor_id) 
            VALUES (?, ?, ?, ?, ?, ?)
            """
            
            params = (
                datetime.now(),
                msg.surtidor_id,
                msg.combustible,
                msg.litros,
                msg.cargas,
                self.id # El ID de este distribuidor
            )
            
            cursor.execute(sql, params)
            new_id = cursor.lastrowid # <--- OBTENER EL ID
            conn.commit()
            conn.close()
            return new_id # <--- RETORNAR EL ID
            
        except Exception as e:
            print(f"Error guardando transacción en BD local: {e}")
            return None

    def _update_transaction_sync_status(self, db_id: int):
        """Marca una transacción como sincronizada en la BD local."""
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            sql = "UPDATE transacciones SET sincronizado_matriz = 1 WHERE id = ?"
            cursor.execute(sql, (db_id,))
            conn.commit()
            conn.close()
            # print(f"Transacción {db_id} marcada como sincronizada.") # Log opcional
        except Exception as e:
            print(f"Error actualizando estado sync de {db_id}: {e}")

    # --- FIN: Funciones de Base de Datos ---

    def start(self):
        """Inicia los dos hilos principales: el servidor y el cliente."""
        
        server_thread = threading.Thread(
            target=self.run_server_for_surtidores, 
            daemon=True
        )
        server_thread.start()
        
        client_thread = threading.Thread(
            target=self.run_client_for_matriz, 
            daemon=True
        )
        client_thread.start()
        
        print(f"📦 Distribuidor '{self.id}' iniciado.")
        print(f"   -> Escuchando surtidores en: {self.host}:{self.port}")
        print(f"   -> Conectando a Matriz en:   {MATRIZ_HOST}:{MATRIZ_PORT}")

    # --- ROL DE SERVIDOR (Escuchando a Surtidores Nivel 1) ---

    def run_server_for_surtidores(self):
        """Abre un puerto y escucha conexiones de los surtidores."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()

        while True:
            try:
                client_socket, addr = self.server_socket.accept()
                print(f"⛽ Nuevo Surtidor conectado desde {addr}")

                with self.lock_surtidores:
                    self.surtidores.append(client_socket)
                
                handler_thread = threading.Thread(
                    target=self.handle_surtidor, 
                    args=(client_socket, addr), 
                    daemon=True
                )
                handler_thread.start()
            
            except Exception as e:
                print(f"Error aceptando conexión de surtidor: {e}")

    def handle_surtidor(self, client_socket, addr):
        """Maneja la comunicación entrante de un solo surtidor."""
        
        self.send_current_prices_to_surtidor(client_socket)
        
        try:
            while True:
                msg_bytes = receive_message(client_socket)
                if msg_bytes is None:
                    print(f"🔌 Surtidor {addr} desconectado.")
                    break
                
                msg_obj = deserialize(msg_bytes)
                
                if isinstance(msg_obj, TransaccionReportMessage):
                    
                    # 1. Guardar en BD Local (SQLite) y obtener ID
                    db_id = self._save_transaction(msg_obj) # <--- ¡CAMBIO!
                    
                    # 2. Log actualizado
                    log_msg = (f"🧾 Reporte de Surtidor {msg_obj.surtidor_id} ({addr}): "
                               f"{msg_obj.litros}L de {msg_obj.combustible} [Guardado en BD id={db_id}]")
                    print(log_msg)

                    # 3. Reenviar la transacción a la Matriz (si está conectada)
                    self.forward_transaction_to_matriz(msg_obj, db_id) # <--- ¡CAMBIO!
                    
                elif isinstance(msg_obj, HeartbeatMessage):
                    print(f"❤️ Heartbeat de Surtidor {msg_obj.id} ({addr})")

        except ConnectionError as e:
            print(f"Error de conexión con Surtidor {addr}: {e}")
        finally:
            with self.lock_surtidores:
                self.surtidores.remove(client_socket)
            client_socket.close()

    def send_current_prices_to_surtidor(self, sock):
        """Envía el caché de precios actual a un surtidor recién conectado."""
        with self.lock_prices:
            if not self.current_prices:
                print(f"Aviso: Surtidor {sock.getpeername()} conectado, pero no hay precios en caché.")
                return

            print(f"Enviando precios de caché a {sock.getpeername()}...")
            for comb, precio in self.current_prices.items():
                msg_obj = PrecioLocalUpdateMessage(comb, precio)
                msg_bytes = serialize(msg_obj)
                framed_msg = frame_message(msg_bytes)
                try:
                    sock.sendall(framed_msg)
                except Exception as e:
                    print(f"Error enviando precio de caché a surtidor: {e}")
                    break
            print("Precios de caché enviados.")

    def broadcast_price_to_surtidores(self, combustible, precio_final):
        """Envía un nuevo precio local a TODOS los surtidores conectados."""
        print(f"TRANSMITIENDO a {len(self.surtidores)} surtidores: {combustible} @ ${precio_final}")
        
        msg_obj = PrecioLocalUpdateMessage(combustible, precio_final)
        msg_bytes = serialize(msg_obj)
        framed_msg = frame_message(msg_bytes)
        
        disconnected = []
        with self.lock_surtidores:
            for sock in self.surtidores:
                try:
                    sock.sendall(framed_msg)
                except Exception:
                    disconnected.append(sock)
            
            for sock in disconnected:
                self.surtidores.remove(sock)
                sock.close()

    # --- ROL DE CLIENTE (Conectando a Matriz Nivel 3) ---

    def run_client_for_matriz(self):
        """Mantiene una conexión persistente a la Matriz y se reconecta si cae."""
        while True: # Bucle de reconexión
            try:
                # 1. Intentar conectar
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((MATRIZ_HOST, MATRIZ_PORT))
                
                print(f"🔗 Conectado exitosamente a la Matriz en {MATRIZ_HOST}:{MATRIZ_PORT}")
                
                with self.lock_matriz_socket:
                    self.socket_to_matriz = sock
                self.is_connected_to_matriz.set() # Pone el flag en "conectado"
                
                # Identificarse ante la Matriz
                self.send_to_matriz(HeartbeatMessage(self.id, "online"))
                
                # --- ¡CAMBIO AÑADIDO! ---
                # 2. Iniciar la sincronización de pendientes
                self._start_sync_thread()
                
                # 3. Iniciar bucle de escucha (era el paso 2)
                self.listen_to_matriz(sock)

            except ConnectionRefusedError:
                print("Matriz no disponible. Operando en modo autónomo.")
            except Exception as e:
                print(f"Error inesperado en conexión con Matriz: {e}")
            finally:
                # 3. Lógica de limpieza y reintento
                self.is_connected_to_matriz.clear() # Pone el flag en "desconectado"
                with self.lock_matriz_socket:
                    if self.socket_to_matriz:
                        self.socket_to_matriz.close()
                    self.socket_to_matriz = None
                
                print(f"Desconectado de la Matriz. Reintentando en {RECONNECT_DELAY} segundos...")
                time.sleep(RECONNECT_DELAY)

    def listen_to_matriz(self, sock: socket.socket):
        """Bucle de recepción de mensajes desde la Matriz."""
        try:
            while True:
                msg_bytes = receive_message(sock)
                if msg_bytes is None:
                    print("Matriz cerró la conexión.")
                    break 
                
                msg_obj = deserialize(msg_bytes)
                
                if isinstance(msg_obj, PrecioUpdateMessage):
                    print(f"💸 Precio base recibido de Matriz: {msg_obj.combustible} @ ${msg_obj.precio_base}")
                    
                    precio_final = int(msg_obj.precio_base * UTILIDAD_FACTOR)
                    
                    with self.lock_prices:
                        self.current_prices[msg_obj.combustible] = precio_final
                    print(f"💰 Precio final local calculado: {msg_obj.combustible} @ ${precio_final}")
                    
                    self.broadcast_price_to_surtidores(msg_obj.combustible, precio_final)
                
        except ConnectionError as e:
            print(f"Error de conexión escuchando a Matriz: {e}")

    # --- Funciones de Comunicación (Nivel 2 -> 3) ---

    def send_to_matriz(self, msg_obj) -> bool:
        """Función helper para enviar un mensaje a la Matriz."""
        if not self.is_connected_to_matriz.is_set():
            return False
            
        with self.lock_matriz_socket:
            if self.socket_to_matriz:
                try:
                    msg_bytes = serialize(msg_obj)
                    framed_msg = frame_message(msg_bytes)
                    self.socket_to_matriz.sendall(framed_msg)
                    return True
                except Exception as e:
                    print(f"Error al enviar a Matriz: {e}")
                    # Si falla el envío, asumimos desconexión
                    self.is_connected_to_matriz.clear() 
                    return False
        return False

    def forward_transaction_to_matriz(self, msg_obj: TransaccionReportMessage, db_id: int | None):
        """Intenta enviar una transacción a la Matriz y actualiza su estado sync."""
        
        if not msg_obj.distribuidor_id:
             msg_obj.distribuidor_id = self.id 
        
        if self.send_to_matriz(msg_obj):
            # --- ¡ÉXITO! La Matriz está online ---
            if db_id:
                # Marcarla como sincronizada en la BD
                self._update_transaction_sync_status(db_id)
        else:
            # --- FALLO: La Matriz está offline ---
            print(f"AVISO: Matriz desconectada. Transacción (id={db_id}) "
                  "guardada en BD local para sincronización futura.")

    # --- Lógica de Sincronización ---

    def _sync_pending_transactions(self):
        """Busca transacciones pendientes y las envía a la Matriz."""
        print("Buscando transacciones pendientes para sincronizar...")
        
        pending_txs = []
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            # Buscar todas las no sincronizadas
            sql = "SELECT id, surtidor_id, combustible, litros, cargas, distribuidor_id FROM transacciones WHERE sincronizado_matriz = 0"
            cursor.execute(sql)
            pending_txs = cursor.fetchall()
            conn.close()
        except Exception as e:
            print(f"Error consultando transacciones pendientes: {e}")
            return # Salir si falla la consulta
        
        if not pending_txs:
            print("No hay transacciones pendientes. Sincronización completa.")
            return
            
        print(f"Se encontraron {len(pending_txs)} transacciones pendientes. Iniciando envío...")
        
        succeeded_count = 0
        for tx in pending_txs:
            # Recrear el objeto de mensaje
            db_id, surtidor_id, combustible, litros, cargas, distribuidor_id = tx
            msg_obj = TransaccionReportMessage(
                surtidor=surtidor_id,
                tipo_combustible=combustible,
                litros=litros,
                cargas=cargas,
                distribuidor_id=distribuidor_id
            )
            
            # Intentar enviar
            if self.send_to_matriz(msg_obj):
                # Si es exitoso, marcar en la BD
                self._update_transaction_sync_status(db_id)
                succeeded_count += 1
                # Pequeña pausa para no saturar
                time.sleep(0.05) 
            else:
                # Si la Matriz se cae *durante* la sincronización
                print("Se perdió la conexión a la Matriz durante la sincronización. Abortando.")
                break # Salir del bucle y reintentar en la próxima reconexión

        print(f"Sincronización finalizada. {succeeded_count} transacciones enviadas.")

    def _start_sync_thread(self):
        """Inicia la sincronización en un hilo separado para no bloquear."""
        sync_thread = threading.Thread(
            target=self._sync_pending_transactions, 
            daemon=True
        )
        sync_thread.start()

# --- Punto de entrada del script ---
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python server_distrib.py <ID_DISTRIBUIDOR> <PUERTO>")
        print("Ejemplo: python server_distrib.py Dist-1 65433")
        sys.exit(1)

    DIST_ID = sys.argv[1]
    DIST_PORT = int(sys.argv[2])

    server = DistribuidorServer(
        id=DIST_ID, 
        host='0.0.0.0', # Escuchar en todas las interfaces
        port=DIST_PORT
    )
    
    server.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Cerrando distribuidor...")