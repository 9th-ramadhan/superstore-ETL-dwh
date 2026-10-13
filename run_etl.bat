@echo off
setlocal enabledelayedexpansion

:: ==============================================================================
:: Windows Task Scheduler Runner: Superstore ETL Pipeline
:: ==============================================================================

:: 1. Pindah ke direktori tempat script ini berada
set "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

:: 2. Pastikan folder logs tersedia
if not exist "%PROJECT_DIR%logs" (
    mkdir "%PROJECT_DIR%logs"
)

:: 3. Dapatkan timestamp standar ISO (YYYYMMDD_HHMMSS)
for /f "usebackq tokens=*" %%a in (`powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"`) do (
    set "TIMESTAMP=%%a"
)
if "%TIMESTAMP%"=="" set "TIMESTAMP=latest"

set "LOG_FILE=%PROJECT_DIR%logs\etl_%TIMESTAMP%.log"
set "LATEST_LOG=%PROJECT_DIR%logs\etl_latest.log"

echo ============================================================================== > "%LOG_FILE%"
echo [START] Menjalankan ETL Pipeline: %DATE% %TIME% >> "%LOG_FILE%"
echo Direktori Proyek : %PROJECT_DIR% >> "%LOG_FILE%"
echo ============================================================================== >> "%LOG_FILE%"

:: 4. Deteksi Executable Python (Anaconda / Venv / Sistem)
if exist "C:\Users\user\anaconda3\python.exe" (
    set "PYTHON_EXE=C:\Users\user\anaconda3\python.exe"
) else if exist "C:\Users\user\anaconda3\envs\hackathon\python.exe" (
    set "PYTHON_EXE=C:\Users\user\anaconda3\envs\hackathon\python.exe"
) else (
    set "PYTHON_EXE=python"
)

echo Menggunakan Python : %PYTHON_EXE% >> "%LOG_FILE%"
echo Memulai eksekusi etl_pipeline.py... >> "%LOG_FILE%"

:: 5. Eksekusi script ETL dan arahkan seluruh output (stdout & stderr) ke log
"%PYTHON_EXE%" "%PROJECT_DIR%etl_pipeline.py" >> "%LOG_FILE%" 2>&1
set "ETL_STATUS=%ERRORLEVEL%"

:: 6. Evaluasi status akhir eksekusi
echo ============================================================================== >> "%LOG_FILE%"
if %ETL_STATUS% EQU 0 (
    echo [SUCCESS] ETL Pipeline selesai dengan sukses pada %DATE% %TIME% >> "%LOG_FILE%"
) else (
    echo [FAILED] ETL Pipeline mengalami kegagalan dengan kode error %ETL_STATUS% pada %DATE% %TIME% >> "%LOG_FILE%"
)
echo ============================================================================== >> "%LOG_FILE%"

:: Salin log ke etl_latest.log agar mudah diakses
copy /y "%LOG_FILE%" "%LATEST_LOG%" >nul 2>&1

:: Tampilkan hasil log ke konsol
type "%LOG_FILE%"

exit /b %ETL_STATUS%
