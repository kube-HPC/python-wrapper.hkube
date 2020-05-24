#!/bin/bash

set -ex

pip install coveralls
coveralls || echo "skipped coveralls"