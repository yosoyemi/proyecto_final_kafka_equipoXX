@echo off
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\iniciar_sistema.ps1"
if errorlevel 1 (
    echo.
    echo No se pudo iniciar todo. Revisa el mensaje anterior y la carpeta logs.
    pause
    exit /b 1
)
echo.
echo El monitor se detuvo. El panel sigue en http://localhost:8501
echo Para apagar producers, consumer, dashboard y Kafka usa DETENER.bat.
pause
