import socket
import threading
import sys
import os
import time
import random

# --- INICIO: Hack para importar 'common' ---
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.append(project_root)

from common.framer import frame_message, receive_message
from common.messages import (
    serialize, deserialize, 
    PrecioLocalUpdateMessage, TransaccionReportMessage, HeartbeatMessage
)
# --- FIN: Hack para importar 'common' ---

# --- Configuración del Surtidor ---
DISTRIBUIDOR_HOST = '127.0.0.1'
RECONNECT_DELAY = 3
COMBUSTIBLES = ["93", "95", "97", "Diesel", "Kerosene"]

class SurtidorClient:
    def __init__(self, id, distrib_port):
        self.id = id
        self.distrib_host = DISTRIBUIDOR_HOST
        self.distrib_port = distrib_port
        
        # --- Estado del Cliente ---
        self.socket_to_distrib = None
        self.lock_socket = threading.Lock() # Lock para el socket
        self.is_connected = threading.Event() # Flag para saber el estado
        
        # --- Estado Operacional del Surtidor ---
        self.local_prices = {} # Caché local de precios. Ej: {'95': 1650}
        self.is_operating = False # Flag para el bloqueo 
        self.pending_price_update = {} # Precios encolados 
        self.lock_state = threading.Lock() # Lock para 'local_prices', 'is_operating' y 'pending'
        
    def start(self):
        """Inicia los hilos de conexión y simulación."""
        
        # Hilo 1: Maneja la conexión (y reconexión) con el Distribuidor
        connect_thread = threading.Thread(
            target=self.run_client_connection, 
            daemon=True
        )
        connect_thread.start()

        # Hilo 2: Simula las ventas de combustible
        simulation_thread = threading.Thread(
            target=self.run_simulation, 
            daemon=True
        )
        simulation_thread.start()
        
        print(f"⛽ Surtidor '{self.id}' iniciado. Intentando conectar a {self.distrib_host}:{self.distrib_port}...")

    def run_client_connection(self):
        """Mantiene una conexión persistente al Distribuidor."""
        while True: # Bucle de reconexión
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.distrib_host, self.distrib_port))
                
                print(f"🔗 Conectado exitosamente al Distribuidor.")
                
                with self.lock_socket:
                    self.socket_to_distrib = sock
                self.is_connected.set() # Pone el flag en "conectado"
                
                # Identificarse ante el Distribuidor
                self.send_to_distrib(HeartbeatMessage(self.id, "online"))
                
                # Iniciar bucle de escucha
                self.listen_to_distrib(sock)

            except ConnectionRefusedError:
                print("Distribuidor no disponible. Reintentando...")
            except Exception as e:
                print(f"Error inesperado en conexión: {e}")
            finally:
                # Lógica de limpieza y reintento
                self.is_connected.clear() # Pone el flag en "desconectado"
                with self.lock_socket:
                    if self.socket_to_distrib:
                        self.socket_to_distrib.close()
                    self.socket_to_distrib = None
                
                print(f"Desconectado del Distribuidor. Reintentando en {RECONNECT_DELAY} seg...")
                time.sleep(RECONNECT_DELAY)

    def listen_to_distrib(self, sock: socket.socket):
        """Bucle de recepción de mensajes desde el Distribuidor."""
        try:
            while True:
                msg_bytes = receive_message(sock)
                if msg_bytes is None:
                    print("Distribuidor cerró la conexión.")
                    break # Rompe el bucle de escucha, lo que activará la reconexión
                
                msg_obj = deserialize(msg_bytes)
                
                if isinstance(msg_obj, PrecioLocalUpdateMessage):
                    # --- Lógica de Actualización de Precio ---
                    self.handle_price_update(msg_obj)
                
        except ConnectionError as e:
            print(f"Error de conexión escuchando a Distribuidor: {e}")

    def handle_price_update(self, msg: PrecioLocalUpdateMessage):
        """
        Aplica o encola una actualización de precio, respetando el 
        requisito de bloqueo operacional. [cite: 81, 58]
        """
        print(f"💸 Precio recibido: {msg.combustible} @ ${msg.precio_final}")
        
        with self.lock_state:
            combustible = msg.combustible
            
            if self.is_operating:
                # --- SURTIDOR OCUPADO: Encolar la actualización ---
                print(f"   -> Surtidor ocupado. Encolando precio de {combustible}.")
                self.pending_price_update[combustible] = msg
            else:
                # --- SURTIDOR LIBRE: Aplicar inmediatamente ---
                self._apply_price_update(msg)
                
    def _apply_price_update(self, msg: PrecioLocalUpdateMessage):
        """Función interna. Asume que el lock_state ya está adquirido."""
        self.local_prices[msg.combustible] = msg.precio_final
        print(f"   -> ¡PRECIO ACTUALIZADO! {msg.combustible} = ${msg.precio_final}")
        # Si estaba pendiente, lo quitamos de la cola
        if msg.combustible in self.pending_price_update:
            del self.pending_price_update[msg.combustible]

    def send_to_distrib(self, msg_obj) -> bool:
        """Función helper para enviar un mensaje al Distribuidor."""
        if not self.is_connected.is_set():
            # print("No conectado, no se puede enviar mensaje.")
            return False
            
        with self.lock_socket:
            if self.socket_to_distrib:
                try:
                    msg_bytes = serialize(msg_obj)
                    framed_msg = frame_message(msg_bytes)
                    self.socket_to_distrib.sendall(framed_msg)
                    return True
                except Exception as e:
                    print(f"Error al enviar a Distribuidor: {e}")
                    return False
        return False

    # --- Hilo de Simulación de Venta ---

    def run_simulation(self):
        """Bucle principal del hilo que simula ventas."""
        while True:
            # Espera un tiempo aleatorio entre ventas
            time.sleep(random.randint(5, 15))
            
            if self.is_connected.is_set():
                self.simulate_sale()

    def simulate_sale(self):
        """
        Simula una única transacción de combustible.
        Esta es la función que implementa el bloqueo operacional.
        """
        
        # 1. Elegir un combustible al azar y ver si tenemos precio
        combustible = random.choice(COMBUSTIBLES)
        
        with self.lock_state:
            if combustible not in self.local_prices:
                print(f"Simulación: No hay precio para {combustible}. Venta cancelada.")
                return
            
            precio_actual = self.local_prices[combustible]
            
            # --- 2. INICIAR OPERACIÓN: Bloquear el surtidor ---
            print(f"--- VENTA INICIADA ({combustible}) ---")
            print(f"   -> [Surtidor '{self.id}' BLOQUEADO]")
            self.is_operating = True
        
        # 3. Simular el tiempo de la venta (fuera del lock)
        #    Esto permite que los mensajes de precio lleguen mientras se "vende".
        try:
            litros = round(random.uniform(5.0, 60.0), 2)
            total_clp = int(litros * precio_actual)
            print(f"   -> Cargando {litros}L de {combustible} por ${total_clp}...")
            time.sleep(random.randint(3, 8)) # Simula el tiempo de carga
            
        except Exception as e:
            print(f"Error durante la simulación de venta: {e}")

        finally:
            # --- 4. FINALIZAR OPERACIÓN: Desbloquear el surtidor ---
            with self.lock_state:
                print(f"   -> [Surtidor '{self.id}' DESBLOQUEADO]")
                self.is_operating = False
                
                # 5. Reportar la transacción al Distribuidor
                report = TransaccionReportMessage(
                    surtidor=self.id,
                    tipo_combustible=combustible,
                    litros=litros,
                    cargas=1 # 1 venta = 1 carga [cite: 80]
                )
                self.send_to_distrib(report)
                print(f"   -> Reporte de transacción enviado.")

                # 6. Aplicar cualquier precio pendiente 
                if self.pending_price_update:
                    print(f"   -> Aplicando {len(self.pending_price_update)} actualizaciones pendientes...")
                    # Aplicamos todas las que estaban en cola
                    for comb in list(self.pending_price_update.keys()):
                        msg = self.pending_price_update[comb]
                        self._apply_price_update(msg)
                
                print("--- VENTA FINALIZADA ---")

# --- Punto de entrada del script ---
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python client_surtidor.py <ID_SURTIDOR> <PUERTO_DISTRIBUIDOR>")
        print("Ejemplo: python client_surtidor.py S-1.1 65433")
        sys.exit(1)

    SURTIDOR_ID = sys.argv[1]
    DIST_PORT = int(sys.argv[2])

    client = SurtidorClient(id=SURTIDOR_ID, distrib_port=DIST_PORT)
    client.start()
    
    # Mantiene el hilo principal vivo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"Cerrando surtidor {SURTIDOR_ID}...")