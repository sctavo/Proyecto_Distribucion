# servidor central para distribuidores
import socket
import threading
import sys
import os

# --- INICIO: Hack para importar 'common' ---
# Esto es necesario porque 'server_matriz.py' está en un subdirectorio
# y necesitamos acceder al paquete 'common' que está en el directorio raíz.

# Obtiene el directorio del script actual (matriz)
script_dir = os.path.dirname(os.path.abspath(__file__))
# Sube un nivel para llegar al directorio raíz del proyecto (Proyecto_Distribucion)
project_root = os.path.dirname(script_dir)
# Añade el directorio raíz al sys.path
sys.path.append(project_root)

# Ahora podemos importar desde 'common'
from common.framer import frame_message, receive_message
from common.messages import (
    serialize, deserialize, PrecioUpdateMessage, 
    TransaccionReportMessage, HeartbeatMessage
)
# --- FIN: Hack para importar 'common' ---


# Constantes
HOST = '127.0.0.1'  # IP local para pruebas
PORT = 65432        # Puerto para la Matriz
COMBUSTIBLES = ["93", "95", "97", "Diesel", "Kerosene"] # Tipos válidos

class MatrizServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_socket = None
        # Lista para guardar los sockets de los distribuidores conectados
        self.distribuidores = []
        # Un "candado" para proteger el acceso a la lista self.distribuidores
        # (ya que será accedida desde múltiples hilos)
        self.lock = threading.Lock()

    def start(self):
        """Inicia el servidor y escucha conexiones."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        print(f"🏠 Servidor Matriz escuchando en {self.host}:{self.port}")

        try:
            while True:
                # Acepta una nueva conexión
                client_socket, addr = self.server_socket.accept()
                print(f"📦 Nueva conexión de Distribuidor desde {addr}")

                # Añade el socket a la lista de forma segura
                with self.lock:
                    self.distribuidores.append(client_socket)

                # Inicia un nuevo hilo para manejar a este cliente
                # El hilo ejecutará la función self.handle_distribuidor
                client_thread = threading.Thread(
                    target=self.handle_distribuidor, 
                    args=(client_socket, addr)
                )
                client_thread.daemon = True # El hilo morirá si el programa principal cierra
                client_thread.start()
                
        except KeyboardInterrupt:
            print("Cerrando servidor Matriz...")
        finally:
            self.server_socket.close()

    def handle_distribuidor(self, client_socket, addr):
        """Maneja la comunicación entrante de un solo distribuidor."""
        try:
            while True:
                # 1. Recibe un mensaje (usando el framer)
                msg_bytes = receive_message(client_socket)

                if msg_bytes is None:
                    # El cliente se desconectó limpiamente
                    print(f"🔌 Distribuidor {addr} desconectado.")
                    break
                
                # 2. Deserializa el mensaje
                msg_obj = deserialize(msg_bytes)

                # 3. Procesa el mensaje
                if isinstance(msg_obj, TransaccionReportMessage):
                    # TODO: Aquí guardaríamos en la BD Central [cite: 95]
                    print(f"📈 Reporte de {addr}: "
                          f"Surtidor {msg_obj.surtidor_id}, "
                          f"{msg_obj.combustible}, {msg_obj.litros}L, {msg_obj.cargas} cargas")
                          
                elif isinstance(msg_obj, HeartbeatMessage):
                    print(f"❤️ Heartbeat de {msg_obj.id} ({addr}): {msg_obj.estado}")
                    
                else:
                    print(f"🤔 Mensaje desconocido de {addr}: {msg_obj}")

        except ConnectionError as e:
            print(f"❌ Error de conexión con {addr}: {e}")
        finally:
            # Sin importar qué pase, removemos al cliente de la lista
            with self.lock:
                self.distribuidores.remove(client_socket)
            client_socket.close()
            print(f"Cerrada conexión con {addr}")

    def broadcast_price(self, combustible, precio_base):
        """Envía una actualización de precio a TODOS los distribuidores."""
        print(f"📣 Transmitiendo nuevo precio: {combustible} a ${precio_base}")
        
        msg_obj = PrecioUpdateMessage(combustible, precio_base)
        msg_bytes = serialize(msg_obj)
        framed_msg = frame_message(msg_bytes)
        
        # Lista de clientes que se desconectaron
        disconnected_clients = []

        with self.lock:
            for sock in self.distribuidores:
                try:
                    sock.sendall(framed_msg)
                except Exception as e:
                    print(f"Error enviando a {sock.getpeername()}: {e}")
                    disconnected_clients.append(sock)

            # Limpiamos la lista de clientes desconectados
            for sock in disconnected_clients:
                self.distribuidores.remove(sock)
                sock.close() # Cerramos el socket
        
        print(f"✅ Precio enviado a {len(self.distribuidores)} distribuidores.")

    def run_admin_cli(self):
        """Simula la Interfaz de Admin  usando la línea de comandos."""
        print("\n--- Interfaz de Admin Matriz ---")
        print("Tipos de combustible: 93, 95, 97, Diesel, Kerosene")
        print("Escriba 'salir' para terminar.")
        
        while True:
            try:
                comb = input("Ingrese combustible: ").strip().capitalize()
                if comb.lower() == 'salir':
                    break
                
                # Validación simple
                if comb not in [c.capitalize() for c in COMBUSTIBLES]:
                    print("Error: Tipo de combustible no válido.")
                    continue
                
                precio_str = input(f"Ingrese precio base para {comb}: ")
                precio = int(precio_str)

                # Llama a la función de broadcast
                self.broadcast_price(comb, precio)

            except ValueError:
                print("Error: El precio debe ser un número entero.")
            except Exception as e:
                print(f"Error inesperado en CLI: {e}")


# --- Punto de entrada del script ---
if __name__ == "__main__":
    server = MatrizServer(HOST, PORT)

    # Inicia el servidor (aceptar conexiones) en un hilo separado
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()

    # El hilo principal se convierte en la Interfaz de Admin
    server.run_admin_cli()

    print("Cerrando programa...")