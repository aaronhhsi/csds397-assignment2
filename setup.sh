#!/bin/bash

echo "Updating system..."
sudo apt update

echo "Installing PostgreSQL..."
sudo apt install postgresql postgresql-contrib -y

echo "Starting PostgreSQL..."
sudo service postgresql start

echo "Configuring PostgreSQL user and database..."
sudo -u postgres psql -c "CREATE USER postgres WITH SUPERUSER PASSWORD 'postgres';"
sudo -u postgres psql -c "CREATE DATABASE csds397 OWNER vscode;"

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Setup complete!"