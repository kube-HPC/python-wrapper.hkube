#!/bin/bash

set -exo pipefail

docker run -d -p 9000:9000 --name minio1 -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" minio/minio server /data
docker run -d --name jaeger1 \
-e COLLECTOR_ZIPKIN_HTTP_PORT=9411 \
-p 5775:5775/udp \
-p 6831:6831/udp \
-p 6832:6832/udp \
-p 5778:5778 \
-p 16686:16686 \
-p 14268:14268 \
-p 14250:14250 \
-p 9411:9411 \
jaegertracing/all-in-one:1.18

until $(curl --output /dev/null --silent --fail http://localhost:16686/api/traces?service=foo); do
    printf '.'
    sleep 1
done
pip install -U -r tests/requirements.txt
python setup.py bdist_wheel
pip install $PWD/dist/*

pylint hkube_python_wrapper
pytest --cov=. --cov-report html tests/
docker rm -f minio1 jaeger1