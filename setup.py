# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import versioneer
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

import sys
sys.path.insert(0, here)  # make sure local files are available to an isolated build

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read().replace('\r\n', '\n')

setup(
    name='openlca2bw',
    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    #version='0.1.8',
    #version=versioneer.get_version(),
    version='1.0.0',
    #cmdclass=versioneer.get_cmdclass(),
    description='A Python package to extract and write an LCA database from OpenLCA to Brightway2',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    author='Cyrille François',
    author_email='cyrille.francois.pro@gmail.com',
    # The project's main homepage.
    url='https://github.com/cyrillefrancois/openlca2bw',

    # Choose your license
    license='BSD-3',

        # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Science/Research',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: BSD License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.8',
    ],

    # What does your project relate to?
    keywords='open life-cycle analysis, openLCA, brightway',


    install_requires=[
        'olca-ipc','brightway2','stats_arrays',
        'urllib3','pyprind','pandas'
        ]
    )
