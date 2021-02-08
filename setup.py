from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="datafetch",
    version="0.0.1",
    author="steph-ben",
    author_email="stephane.benchimol@gmail.com",
    description="Tools for fetching data, and providing ready-to-use [Prefect](https://prefect.io) flows",
    url="https://github.com/steph-ben/datafetch",
    packages=[
        'datafetch',
    ],
    install_requires=requirements,
)
