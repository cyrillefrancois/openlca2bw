# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
      name='openlca2bw',
      version='1.0',
      description='A Python package to extract and write an LCA database from OpenLCA to Brightway2',
      packages=find_packages(),
      author='Cyrille François',
      install_requires=[
          'olca-ipc','brightway2','stats_arrays',
          'urllib3','pyprind','pandas'
          ]
      )
