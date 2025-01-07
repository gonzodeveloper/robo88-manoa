#!/usr/bin/env python

from setuptools import setup, find_packages

# Get dependencies
with open("requirements.txt") as f:
    install_requires = f.read().splitlines()

setup(
    name="robo88",
    version="0.1.1",
    author="Kyle Hart",
    author_email="kylehart@hawaii.edu",
    license="GPL v.3",
    packages=find_packages(),
    install_requires=install_requires,
    zip_safe=False,
)
