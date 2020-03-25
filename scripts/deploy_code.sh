#!/bin/bash

# Fill in with the IP address of the Airflow server
server = "127.0.0.1"

# AWS private key
key_path = "/path/to/key.pem"

# Package up the project files
mkdir target
zip -r --exclude=*__pycache__* target/project_files.zip src/ config/

# Synchronises changes to an external server
rsync -rzhvtP \
    --delete \
    --exclude ".venv" \
    --exclude "__pycache__" \
    --exclude ".pytest_cache" \
    --exclude ".git" \
    --exclude ".idea" \
    -e "ssh -i $key_path" \
    $1 $2@$server:$3
