#!/bin/bash
# Setup script for PFAS Report Monitor

echo "=================================="
echo "PFAS Report Monitor - Setup"
echo "=================================="
echo ""

# Check Python version
echo "Checking Python..."
python3 --version || { echo "ERROR: Python 3 is required"; exit 1; }

# Create virtual environment (recommended)
echo ""
echo "Creating virtual environment..."
python3 -m venv venv

# Activate it
source venv/bin/activate

# Install dependencies
echo ""
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Install Playwright browsers
echo ""
echo "Installing Playwright browser (Chromium)..."
playwright install chromium

echo ""
echo "=================================="
echo "Setup Complete!"
echo "=================================="
echo ""
echo "To use the tool:"
echo ""
echo "1. Activate the virtual environment:"
echo "   source venv/bin/activate"
echo ""
echo "2. Run the interactive explorer first to verify it works:"
echo "   python interactive_explorer.py"
echo ""
echo "3. Then use the monitor:"
echo "   python pfas_monitor.py --check"
echo ""
