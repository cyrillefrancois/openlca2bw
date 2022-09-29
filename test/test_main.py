# -*- coding: utf-8 -*-
"""
Created on Thurs Sept 29 09:12:46 2022

@author: shirubana

Using pytest to create unit tests for openlca2bw.
to run unit tests, run pytest from the command line in the openlca2bw directory
to run coverage tests, run py.test --cov-report term-missing --cov=openlca2bw

"""

import openlca2bw as olca2bw
import brightway2 as bw
import pytest
import os


# try navigating to tests directory so tests run from here.
try:
    os.chdir('tests')
except:
    pass


TESTDIR = os.path.dirname(__file__)  # this folder

# test the code with the dummy database in the /test/ directory
TESTDATABASE = r'OLCAdb_demo'

def test_load_openLCA_Json():

    olca2bw.load_openLCA_Json(path_zip=os.path.join(TESTDIR,TESTDATABASE),
                            project_name='MLDB',
                            nonuser_db_name = 'MLDB',
                            overwrite = True, verbose=True)
                            
    bio = bw.Database('bioshpere3')
    db = bw.Database('MLDB')

    assert len(bio) == 0 # Change this once the DB or process is cleaned up. 
    assert len(db) == 0
    