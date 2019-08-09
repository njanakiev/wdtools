from setuptools import find_packages, setup
import os.path


setup(
    name="wdtools",
    version="0.1.0",
    packages=["wdtools"],
    install_requires=["requests",
                      "numpy",
                      "pandas",
                      "simplejson"]
)
