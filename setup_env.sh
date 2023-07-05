#!/bin/bash

echo "Installing python pip"
sudo apt install python3-pip

pip install pipenv

mkdir .env

pipenv shell

echo "Install dependencies"
pipenv install numpy kafka-python pytest mock pre-commit

pre-commit install

sudo docker-compose up
