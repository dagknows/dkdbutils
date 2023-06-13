#!/bin/bash

if [ ! -d "./venv" ]; then
  virtualenv venv
fi

deactivate
source ./venv/bin/activate
pip install -r requirements.txt
pip install -r dev_requirements.txt
