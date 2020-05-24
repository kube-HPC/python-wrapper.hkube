#!/bin/bash

set -ex

docker run -d -p 9000:9000 --name minio1 -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" minio/minio server /data

pip install -U -r tests/requirements.txt
python setup.py bdist_wheel
pip install $PWD/dist/*

pylint hkube_python_wrapper
pytest --cov=. --cov-report html tests/