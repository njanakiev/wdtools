from setuptools import find_packages, setup
import os.path

# The directory containing this file
HERE = os.path.abspath(os.path.dirname(__file__))

# The text of the README file
with open(os.path.join(HERE, "README.rst")) as fid:
    README = fid.read()

setup(
    name="wdtools",
    version="0.1.0",
    description="Wikidata utility functions and tools",
    long_description=README,
    url="https://github.com/njanakiev/wdtools",
    author="Nikolai Janakiev",
    author_email="nikolai.janakiev@gmail.com",
    license="MIT",
    packages=["wdtools"],
    install_requires=["requests",
                      "pandas",
                      "geopandas",
                      "shapely",
                      "sqlalchemy",
                      "geoalchemy2",
                      "simplejson"],
    platforms="any",
    include_package_data=True
)
