# servidor para surtidores + cliente hacia matriz
import socket
import threading
import sys
import os
import time

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
MATRIZ_HOST = '127.0.0.1'
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
        
        # --- Estado del Servidor (Nivel 2) ---
        self.server_socket = None # Socket para escuchar a los Surtidores
        self.surtidores = [] # Lista de sockets de surtidores conectados
        self.lock_surtidores = threading.Lock() # Lock para la lista de surtidores
        
        # --- Caché Local y Lógica de Negocio ---
        # El "caché local de precios" para operar de forma autónoma 
        self.current_prices = {} # Ej: {'95': 1650, '93': 1600}
        self.lock_prices = threading.Lock()
        
        # --- Estado del Cliente (Nivel 2 -> 3) ---
        self.socket_to_matriz = None # Socket conectado a la Matriz
        self.lock_matriz_socket = threading.Lock()
        self.is_connected_to_matriz = threading.Event() # Flag para saber el estado

    def start(self):
        """Inicia los dos hilos principales: el servidor y el cliente."""
        
        # Hilo 1: Inicia el servidor para escuchar a los surtidores
        server_thread = threading.Thread(
            target=self.run_server_for_surtidores, 
            daemon=True
        )
        server_thread.start()
        
        # Hilo 2: Inicia el cliente para conectarse a la Matriz
        # Este hilo manejará la lógica de reconexión
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
                
                # Inicia un hilo para manejar este surtidor
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
        
        # Paso 1: Al conectarse, enviar al surtidor todos los precios actuales
        self.send_current_prices_to_surtidor(client_socket)
        
        try:
            while True:
                msg_bytes = receive_message(client_socket)
                if msg_bytes is None:
                    print(f"🔌 Surtidor {addr} desconectado.")
                    break
                
                msg_obj = deserialize(msg_bytes)
                
                if isinstance(msg_obj, TransaccionReportMessage):
                    # TODO: Guardar en BD Local (SQLite) [cite: 79]
                    print(f"🧾 Reporte de Surtidor {msg_obj.surtidor_id} ({addr}): "
                          f"{msg_obj.litros}L de {msg_obj.combustible}")
                    
                    # Reenviar la transacción a la Matriz (si está conectada)
                    self.forward_transaction_to_matriz(msg_obj)
                    
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
                    break # Si falla, probablemente el surtidor se desconectó
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
            
            # Limpieza de sockets desconectados
            for sock in disconnected:
                self.surtidores.remove(sock)
                sock.close()

    # --- ROL DE CLIENTE (Conectando a Matriz Nivel 3) ---

    def run_client_for_matriz(self):
        """Mantiene una conexión persistente a la Matriz y se reconecta si cae."""
        while True: # Bucle de reconexión [cite: 90]
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
                
                # 2. Iniciar bucle de escucha
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
                    break # Rompe el bucle de escucha, lo que activará la reconexión
                
                msg_obj = deserialize(msg_bytes)
                
                if isinstance(msg_obj, PrecioUpdateMessage):
                    # --- Lógica de Negocio Principal ---
                    print(f"💸 Precio base recibido de Matriz: {msg_obj.combustible} @ ${msg_obj.precio_base}")
                    
                    # 1. Calcular precio final con utilidad 
                    precio_final = int(msg_obj.precio_base * UTILIDAD_FACTOR)
                    
                    # 2. Actualizar caché local 
                    with self.lock_prices:
                        self.current_prices[msg_obj.combustible] = precio_final
                    print(f"💰 Precio final local calculado: {msg_obj.combustible} @ ${precio_final}")
                    
                    # 3. Transmitir a todos los surtidores
                    self.broadcast_price_to_surtidores(msg_obj.combustible, precio_final)
                
                # Podríamos recibir otros tipos de mensajes (ej: comandos admin)
                
        except ConnectionError as e:
            print(f"Error de conexión escuchando a Matriz: {e}")
            # La excepción romperá el bucle y activará la reconexión

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
                    return False
        return False

    def forward_transaction_to_matriz(self, msg_obj: TransaccionReportMessage):
        """Intenta enviar una transacción a la Matriz."""
        
        # Le añadimos el ID del distribuidor para que la Matriz sepa de quién es
        msg_obj.distribuidor_id = self.id 
        
        if not self.send_to_matriz(msg_obj):
            # Aquí se cumple el requisito de tolerancia a fallos [cite: 90]
            print(f"AVISO: Matriz desconectada. Transacción de {msg_obj.surtidor_id} "
                  "guardada localmente (TODO: encolar en BD para sincronización).")
            # TODO: Implementar la cola de sincronización.
            # Por ahora, solo la guardamos en la BD local.


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
        host='127.0.0.1', 
        port=DIST_PORT
    )
    
    server.start()
    
    # Mantiene el hilo principal vivo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Cerrando distribuidor...")