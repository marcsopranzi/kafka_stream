#!/bin/bash

echo "Installing python vevn "
sudo apt install python3.10-venv

echo "Installing python pip"
sudo apt install python3-pip

echo "Installing venv"
python3 -m venv venv_kafka_stream

echo "Activating venv"
source ./venv_kafka_stream/bin/activate

echo "Install dependencies"
pip install numpy kafka-python dash pymongo[srv]

echo "Pip upgrade"
ppython3 -m pip install --upgrade pip