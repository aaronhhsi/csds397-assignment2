#!/bin/bash

echo "Updating system..."
sudo apt update

echo "Installing PostgreSQL..."
sudo apt install postgresql postgresql-contrib -y

echo "Starting PostgreSQL..."
sudo service postgresql start

echo "Configuring PostgreSQL user and database..."
sudo su postgres -s /bin/bash -c "psql -c \"ALTER USER postgres WITH PASSWORD 'postgres';\""
sudo su postgres -s /bin/bash -c "psql -c \"CREATE DATABASE csds397 OWNER postgres;\""

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Setup complete!"