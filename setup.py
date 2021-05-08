import os
from setuptools import setup, find_packages

packages = find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"])

reqs = []
with open("requirements.txt") as f:
    for line in f:
        if line.startswith('git+https'):
            continue
        reqs.append(line.strip())

setup(
    name='weather_pipeline',
    version="0.0.0",
    description='Weather Task TR-GMBH',
    classifiers=[
        "Programming Language :: Python"
    ],
    author='Prakhar Srivastava',
    author_email='srivastavaprakhar1@gmail.com',
    packages=packages,
    install_requires=reqs
)