#!/usr/bin/env python
from __future__ import print_function, division, absolute_import

import setuptools
import os

here = os.path.abspath(os.path.dirname(__file__))

VERSION = '2.3.0'





packages = setuptools.find_packages()

requires = [
    'Events==0.4',
    'websocket-client==0.57.0',
    'simplejson==3.17.2',
    'pymongo==3.11.3',
    'msgpack==1.0.2',
    'boto3<=1.17.20',
    "wsaccel==0.6.2",
    "six==1.15.0",
    'pyzmq<=22.0.3',
    'jaeger-client==4.0.0; python_version < "3.2.0"',
    'jaeger-client>=4.4.0; python_version >= "3.2.0"',
    "pympler==0.9"
]

with open("README.md", "r") as f:
    long_description = f.read()

about = {}
with open(os.path.join(here, 'hkube_python_wrapper', '__version__.py'), 'r') as f:
    exec(f.read(), about)

setuptools.setup(
    name=about['__title__'],
    version=VERSION,
    author=about['__author__'],
    author_email=about['__author_email__'],
    description=about['__description__'],
    license=about['__license__'],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=about['__url__'],
    packages=packages,
    install_requires=requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
