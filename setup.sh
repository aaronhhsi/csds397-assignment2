#!/bin/bash

echo "Updating system..."
sudo apt update

echo "Installing PostgreSQL..."
sudo apt install postgresql postgresql-contrib -y

echo "Starting PostgreSQL..."
sudo service postgresql start

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Setup complete!"