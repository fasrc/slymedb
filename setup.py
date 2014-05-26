"""
Created on May 24, 2014
Copyright (c) 2014
Harvard FAS Research Computing
All rights reserved.

@author: Aaron Kitzmiller
"""
import os
from setuptools import setup, find_packages

setup(
    name = "slymedb",
    version = "0.1.0",
    author='Aaron Kitzmiller <aaron_kitzmiller@harvard.edu>',
    author_email='aaron_kitzmiller@harvard.edu',
    description='Uses slyme modules to build a job report database',
    license='LICENSE.txt',
    keywords = "slurm",
    url='http://pypi.python.org/pypi/slymedb/',
    packages = find_packages(),
    long_description=open('README.txt').read(),
    install_requires=[
        "SQLAlchemy > 0.9.0",
        "slyme >= 0.1.0"
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
    ],
)
