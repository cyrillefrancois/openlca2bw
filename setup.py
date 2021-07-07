# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
      name='openlca2bw',
      version='0.1',
      description='A Python package to extract and write an LCA database from OpenLCA to Brightway2',
      packages=find_packages(),
      author='Cyrille François',
      install_requires=[
          'olca-ipc','brightway2',
          'urllib3','pyprind','pandas'
          ]
      )
