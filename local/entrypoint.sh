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

# Check workspace directory structure
echo "Checking workspace directory structure..."
if [ ! -d "/home/glue_user/workspace" ]; then
    echo "Error: /home/glue_user/workspace directory not found"
    exit 1
fi

# Verify Python packages
echo "Verifying Python packages..."
python3 -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"
python3 -c "import jupyterlab; print(f'JupyterLab version: {jupyterlab.__version__}')"

echo "Starting Jupyter Lab..."
cd /home/glue_user/workspace
python3 -m jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --notebook-dir=/home/glue_user/workspace

# If Jupyter fails, keep container alive for troubleshooting
if [ $? -ne 0 ]; then
    echo "Error starting Jupyter Lab. Container will remain running for troubleshooting."
    echo "You can connect to the container using: docker exec -it glue_local bash"
    tail -f /dev/null
fi
