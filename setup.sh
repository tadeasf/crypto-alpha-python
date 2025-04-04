#!/bin/bash

# Exit on error
set -e

echo "Setting up crypto-alpha-python..."

# Install dependencies using rye
echo "Installing dependencies with rye..."
rye sync

# Install pip in virtual environment
echo "Installing pip in virtual environment..."
.venv/bin/python -m ensurepip --upgrade

# Uninstall existing greenlet
echo "Uninstalling existing greenlet..."
.venv/bin/python -m pip uninstall -y greenlet

# Install greenlet from source
echo "Installing greenlet from source..."
.venv/bin/python -m pip install --no-binary :all: greenlet==3.1.1

# Verify greenlet installation
echo "Verifying greenlet installation..."
.venv/bin/python -c "import greenlet._greenlet; print('Greenlet C extension is available!')"

echo "Setup complete! You can now run the application with 'rye run start'" 