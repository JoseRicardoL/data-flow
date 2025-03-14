#!/bin/bash
set -e

echo "Starting Glue Emulator environment..."

# Display debug information
echo "System information:"
uname -a
echo "Python path:"
which python3
echo "Python version:"
python3 --version

# Install requirements if they exist
if [ -f /workspace/local/requirements.txt ]; then
    echo "Installing Python packages from requirements.txt..."
    pip install --user -r /workspace/local/requirements.txt
else
    echo "No requirements.txt found, skipping package installation."
fi

echo "Starting Jupyter Lab..."
python3 -m jupyter lab --ip=0.0.0.0 --port=8888 --no-browser

# If Jupyter fails, keep container alive for troubleshooting
if [ $? -ne 0 ]; then
    echo "Error starting Jupyter Lab. Container will remain running for troubleshooting."
    tail -f /dev/null
fi