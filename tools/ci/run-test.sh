#!/bin/bash

set -ex

pylint hkube_python_wrapper

pytest --cov=. --cov-report html tests/