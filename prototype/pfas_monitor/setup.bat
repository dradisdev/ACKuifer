@echo off
REM Setup script for PFAS Report Monitor (Windows)

echo ==================================
echo PFAS Report Monitor - Setup
echo ==================================
echo.

REM Check Python
echo Checking Python...
python --version || (
    echo ERROR: Python 3 is required
    echo Download from https://python.org
    pause
    exit /b 1
)

REM Create virtual environment
echo.
echo Creating virtual environment...
python -m venv venv

REM Activate it
call venv\Scripts\activate.bat

REM Install dependencies
echo.
echo Installing Python dependencies...
pip install -r requirements.txt

REM Install Playwright browsers
echo.
echo Installing Playwright browser (Chromium)...
playwright install chromium

echo.
echo ==================================
echo Setup Complete!
echo ==================================
echo.
echo To use the tool:
echo.
echo 1. Activate the virtual environment:
echo    venv\Scripts\activate.bat
echo.
echo 2. Run the interactive explorer first:
echo    python interactive_explorer.py
echo.
echo 3. Then use the monitor:
echo    python pfas_monitor.py --check
echo.
pause
