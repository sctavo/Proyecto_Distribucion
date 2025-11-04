# Usar una imagen de Python ligera
FROM python:3.11-slim

# Instalar las dependencias de Tkinter (para la GUI de la Matriz)
RUN apt-get update && apt-get install -y tk

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar TODO el proyecto (common/, matriz/, etc.) al contenedor
COPY . .

# Configurar la variable DISPLAY para que la GUI del contenedor...
# ...se dibuje en el "X Server" de la máquina host (Windows).
# 'host.docker.internal' es un nombre especial que usa Docker Desktop.
ENV DISPLAY=host.docker.internal:0.0