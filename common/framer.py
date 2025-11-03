# utilidades de framing (longitud-prefijo) y JSON
import socket
import struct

# Usamos '!I' para el formato del struct:
# ! = Network byte order (big-endian), estándar para redes.
# I = Unsigned Integer (4 bytes).
# Esto nos permite manejar mensajes de hasta 4GB.
HEADER_FORMAT = "!I"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

def frame_message(message_bytes: bytes) -> bytes:
    """
    Agrega un prefijo de 4 bytes con la longitud del mensaje.
    """
    # 1. Empaqueta la longitud del mensaje en 4 bytes
    header = struct.pack(HEADER_FORMAT, len(message_bytes))
    # 2. Retorna [Header de 4 bytes] + [Mensaje]
    return header + message_bytes

def _read_n_bytes(sock: socket.socket, n: int) -> bytes:
    """
    Función de ayuda para leer exactamente 'n' bytes de un socket.
    Maneja el caso en que sock.recv() devuelva menos bytes de los pedidos.
    """
    data_buffer = b""
    while len(data_buffer) < n:
        # Pide los bytes restantes
        remaining = n - len(data_buffer)
        chunk = sock.recv(remaining)
        
        if not chunk:
            # El socket se cerró inesperadamente
            raise ConnectionError("Socket desconectado mientras se leía el mensaje.")
            
        data_buffer += chunk
    return data_buffer

def receive_message(sock: socket.socket) -> bytes | None:
    """
    Recibe un mensaje completo con prefijo de longitud desde un socket.
    Retorna los bytes del mensaje, o None si el cliente se desconectó limpiamente.
    """
    try:
        # 1. Leer el header (4 bytes) para obtener la longitud
        header_data = _read_n_bytes(sock, HEADER_SIZE)
        
        # 2. Desempacar el header para obtener el entero de la longitud
        (message_length,) = struct.unpack(HEADER_FORMAT, header_data)
        
        # 3. Leer exactamente 'message_length' bytes
        message_bytes = _read_n_bytes(sock, message_length)
        
        return message_bytes
        
    except ConnectionError as e:
        # El socket se cerró inesperadamente (manejado en _read_n_bytes)
        print(f"Error de conexión: {e}")
        return None
    except struct.error as e:
        print(f"Error de struct (posiblemente header malformado): {e}")
        return None
    except Exception as e:
        # Maneja el caso de desconexión limpia (recv() devuelve b"")
        # Esto sucede si _read_n_bytes falla al leer el header
        # porque el cliente cerró la conexión.
        # print("Cliente desconectado limpiamente.")
        return None