#!/bin/bash

echo "Installing python vevn "
sudo apt install python3.10-venv

echo "Installing python pip"
sudo apt install python3-pip

pip install pipenv

pipenv shell

echo "Install dependencies"
pipenv install numpy kafka-python
