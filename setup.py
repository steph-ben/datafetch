from pathlib import Path

from setuptools import setup, find_packages


basedir = Path(__file__).parent
readme_content = (basedir / "README.md").read_text()
requirements = (basedir / "requirements.txt").read_text()


setup(
    name="datafetch",
    version="0.0.2",
    author="steph-ben",
    author_email="stephane.benchimol@gmail.com",
    description="Tools for fetching data, and providing ready-to-use https://prefect.io flows",
    long_description=readme_content,
    long_description_content_type="text/markdown",
    url="https://github.com/steph-ben/datafetch",
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
    ]
)
