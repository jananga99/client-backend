#!/bin/bash

python3 -m venv .venv
pip install -r requirements.txt
source .venv/bin/activate
python3 pre.py
