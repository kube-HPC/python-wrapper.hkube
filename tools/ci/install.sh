#!/bin/bash

set -ex

pip install -U -r tests/requirements.txt
python setup.py bdist_wheel
pip install $PWD/dist/*