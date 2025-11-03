# run_local_all.ps1
# Script para lanzar todo el sistema en modo local para pruebas.
# -----------------------------------------------------------------

Write-Host "Lanzando el sistema de distribucion de combustible en modo local..." -ForegroundColor Yellow

$ProjectRoot = "C:\Users\gsgsn\OneDrive\Desktop\Proyecto_Distribucion"

# --- 0. Establece el directorio de trabajo (para este script) ---
$ErrorActionPreference = "Stop"
try {
    Set-Location $ProjectRoot
    Write-Host "Establecido el directorio de trabajo en: $(Get-Location)" -ForegroundColor Green
} catch {
    Write-Host "ERROR: No se pudo encontrar el directorio del proyecto en '$ProjectRoot'." -ForegroundColor Red
    pause
    exit
}

# --- 1. Lanzar el Servidor Matriz (Nivel 3) ---
Write-Host "Iniciando Nivel 3: Servidor Matriz..."
$CmdMatriz = "Set-Location -Path '$ProjectRoot'; py.exe matriz\server_matriz.py"
Start-Process wt.exe -ArgumentList "new-tab", "-t", "MATRIZ (Nivel 3)", "powershell.exe", "-NoExit", "-Command", $CmdMatriz

Start-Sleep -Seconds 2

# --- 2. Lanzar el Distribuidor 1 (Nivel 2) ---
Write-Host "Iniciando Nivel 2: Distribuidor 1 (en puerto 65433)..."
$CmdDist1 = "Set-Location -Path '$ProjectRoot'; py.exe distribuidor\server_distrib.py Dist-1 65433"
Start-Process wt.exe -ArgumentList "new-tab", "-t", "Distribuidor-1", "powershell.exe", "-NoExit", "-Command", $CmdDist1

# --- 3. Lanzar Surtidores para el Distribuidor 1 (Nivel 1) ---
Write-Host "Iniciando Nivel 1: Surtidor 1.1 (conectado a 65433)..."
$CmdSurt1 = "Set-Location -Path '$ProjectRoot'; py.exe surtidor\client_surtidor.py S-1.1 65433"
Start-Process wt.exe -ArgumentList "new-tab", "-t", "Surtidor 1.1", "powershell.exe", "-NoExit", "-Command", $CmdSurt1

# --- 4. Lanzar el Distribuidor 2 (Nivel 2) ---
Write-Host "Iniciando Nivel 2: Distribuidor 2 (en puerto 65434)..."
$CmdDist2 = "Set-Location -Path '$ProjectRoot'; py.exe distribuidor\server_distrib.py Dist-2 65434"
Start-Process wt.exe -ArgumentList "new-tab", "-t", "Distribuidor-2", "powershell.exe", "-NoExit", "-Command", $CmdDist2

# --- 5. Lanzar Surtidores para el Distribuidor 2 (Nivel 1) ---
Write-Host "Iniciando Nivel 1: Surtidor 2.1 (conectado a 65434)..."
$CmdSurt2 = "Set-Location -Path '$ProjectRoot'; py.exe surtidor\client_surtidor.py S-2.1 65434"
Start-Process wt.exe -ArgumentList "new-tab", "-t", "Surtidor 2.1", "powershell.exe", "-NoExit", "-Command", $CmdSurt2

Write-Host "Sistema lanzado. Deberias tener 5 pestañas abiertas." -ForegroundColor Cyan