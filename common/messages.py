# tipos de mensajes y validación de mensajes
import json

# --- Clases de Mensajes (Estructuras de datos) ---
# Estas clases son "data classes" simples para definir la estructura
# de nuestros mensajes, basadas en tu tabla.

class PrecioUpdateMessage:
    """Matriz -> Distribuidor"""
    def __init__(self, tipo_combustible, precio_base):
        self.tipo = "PRECIO_UPDATE"
        self.combustible = tipo_combustible
        self.precio_base = precio_base
    
    def __repr__(self):
        return f"PrecioUpdate(comb={self.combustible}, base=${self.precio_base})"

class PrecioLocalUpdateMessage:
    """Distribuidor -> Surtidor"""
    def __init__(self, tipo_combustible, precio_final):
        self.tipo = "PRECIO_LOCAL"
        self.combustible = tipo_combustible
        self.precio_final = precio_final

    def __repr__(self):
        return f"PrecioLocalUpdate(comb={self.combustible}, final=${self.precio_final})"

class TransaccionReportMessage:
    """Surtidor -> Distribuidor"""
    def __init__(self, surtidor, tipo_combustible, litros, cargas):
        # NOTA: Ajusté los campos según los requisitos.
        # El surtidor reporta litros y cargas[cite: 80].
        # Tu avance mencionaba 'litros' y 'cargas'  (aunque el ejemplo solo usaba litros).
        # Usaremos ambos para ser más completos.
        self.tipo = "TRANSACCION"
        self.surtidor_id = surtidor
        self.combustible = tipo_combustible
        self.litros = litros
        self.cargas = cargas
        
    def __repr__(self):
        return f"Transaccion(surtidor={self.surtidor_id}, comb={self.combustible}, {self.litros}L, {self.cargas} cargas)"

class HeartbeatMessage:
    """Bidireccional"""
    def __init__(self, id, estado):
        self.tipo = "HEARTBEAT"
        self.id = id
        self.estado = estado

    def __repr__(self):
        return f"Heartbeat(id={self.id}, estado={self.estado})"

# --- Serialización y Deserialización ---

def serialize(message_obj) -> bytes:
    """Convierte un objeto de mensaje en bytes (JSON codificado en UTF-8)."""
    try:
        message_dict = message_obj.__dict__
        message_json = json.dumps(message_dict)
        return message_json.encode('utf-8')
    except Exception as e:
        print(f"Error serializando mensaje: {e}")
        return b""

def deserialize(message_bytes: bytes):
    """
    Convierte bytes (JSON codificado en UTF-8) en un objeto de mensaje específico.
    Esta es una "factory function" que lee el campo "tipo" y devuelve la clase correcta.
    """
    try:
        message_json = message_bytes.decode('utf-8')
        data = json.loads(message_json)
        
        msg_type = data.pop("tipo", None) # Extrae el tipo y lo quita del dict

        if msg_type == "PRECIO_UPDATE":
            ## Mapeamos 'combustible' (del JSON) a 'tipo_combustible' (del constructor)
            data['tipo_combustible'] = data.pop('combustible')
            return PrecioUpdateMessage(**data)
            
        elif msg_type == "PRECIO_LOCAL":
            data['tipo_combustible'] = data.pop('combustible')
            return PrecioLocalUpdateMessage(**data)
            
        elif msg_type == "TRANSACCION":
            # Renombramos 'surtidor_id' a 'surtidor' para el constructor
            data['surtidor'] = data.pop('surtidor_id') 
            data['tipo_combustible'] = data.pop('combustible')
            return TransaccionReportMessage(**data)
            
        elif msg_type == "HEARTBEAT":
            return HeartbeatMessage(**data)
            
        else:
            print(f"Error: Tipo de mensaje desconocido: {msg_type}")
            return None
            
    except json.JSONDecodeError:
        print("Error: Mensaje JSON mal formado.")
        return None
    except Exception as e:
        print(f"Error deserializando mensaje: {e}")
        return None