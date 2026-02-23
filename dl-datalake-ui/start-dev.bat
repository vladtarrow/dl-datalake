@echo off
echo Starting DL-Datalake UI Infrastructure...

:: Start Backend in a new window
echo Launching Backend (FastAPI on port 8000)...
start "DL-Lake Backend" cmd /k "cd backend && title Backend && py -m uvicorn main:app --reload --port 8000"

:: Start Frontend in a new window
echo Launching Frontend (Vite on port 5173)...
start "DL-Lake Frontend" cmd /k "cd frontend && title Frontend && npm run dev"

echo.
echo ==========================================
echo Backend:  http://localhost:8000
echo Frontend: http://localhost:5173
echo ==========================================
echo Keep this window open or close it (processes are running in separate windows).
pause
