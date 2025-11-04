# ⛽ Proyecto 2: Sistema Distribuido de Bencineras

Implementación de un sistema distribuido de 3 niveles (Matriz, Distribuidor, Surtidor) para el curso de **Sistemas Distribuidos**. El sistema gestiona la asignación de precios de combustible de forma centralizada y recopila reportes de ventas de forma tolerante a fallos.

El proyecto simula la red de una compañía de combustibles, permitiendo a la Casa Matriz (Nivel 3) fijar precios, a los Distribuidores (Nivel 2) aplicar márgenes y gestionar sus Surtidores (Nivel 1), los cuales simulan ventas y reportan transacciones.

## 🛠️ Características Principales

**Arquitectura de 3 Niveles:** Servidor Matriz (GUI), Servidor Distribuidor (Lógica de Negocio) y Cliente Surtidor (Simulación).
**Comunicación por Sockets TCP:** Protocolo de mensajería basado en JSON  con *framing* de mensajes (prefijo de longitud) para una comunicación fiable.
**Tolerancia a Fallos y Sincronización:** El Distribuidor puede operar 100% offline (modo autónomo) si pierde conexión con la Matriz. [cite_start]Al reconectarse, sincroniza automáticamente todas las transacciones pendientes guardadas localmente.
**Persistencia de Datos:** Uso de bases de datos **SQLite** tanto en la Matriz (para reportes centralizados) como en el Distribuidor (para tolerancia a fallos).
**GUI de Administración:** La Matriz posee una interfaz gráfica simple (con **Tkinter**) para enviar precios, ver logs en vivo y generar reportes de ventas.
**Bloqueo Operacional:** Un surtidor no puede actualizar su precio si se encuentra en medio de una venta, encolando la actualización para aplicarla al finalizar.
* **Pruebas Locales y en Red:** El sistema se puede ejecutar de dos formas:
    1.  **Modo Local:** Usando un script de PowerShell (`scripts/run_local_all.ps1`) que lanza todos los componentes en la máquina local.
    2.  **Modo Red (Contenedores):** Usando `docker-compose up` para lanzar cada componente en un contenedor separado, simulando una red real.

## 🔧 Tecnologías Utilizadas

* **Backend:** Python 3.11
* **Red:** `socket` (TCP), `json`, `struct` (para *framing*)
* **Concurrencia:** `threading`
* **Base de Datos:** `sqlite3`
* **GUI:** `tkinter` (y `ttk`)
* **Contenerización:** Docker y Docker Compose
