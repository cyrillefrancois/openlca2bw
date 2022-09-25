# -*- coding: utf-8 -*-
"""
Created on Fri Jul 16 10:06:05 2021

@author: cyrille.francois
"""

import zipfile
import os
import json
from .extract import Extraction_functions
import pyprind
import shutil

class Json_Extractor(Extraction_functions):
    def __init__(self):
        self.categories = []
        self.unit_groups = []
        self.flow_properties = []
        self.locations = []
        self.flows = []
        self.lcia_categories = []
        self.lcia_methods = []
        self.processes = []
        self.parameters = []
        self.flow_unit = None
        self.unit_conv = None
        self.location_table = None
        self.change_param = {}
        self.convert_ids = {}


    def extract_zip_openlca(self,zip_path=str,
                            folders=['categories','unit_groups','flow_properties','locations','flows','lcia_categories','lcia_methods','processes','parameters']):
        
        if zip_path[-4:] == ".zip":
            storage_path = os.path.join(os.path.dirname(zip_path),'olca2bw_Unzip')
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    listOfFileNames = zip_ref.namelist()
                    pbar = pyprind.ProgBar(len(folders), title="Extracting json files from OpenLCA zip folder:")
                    for dir in folders:
                        pbar.update(item_id = dir.ljust(15),force_flush=True)
                        listOfExtractFiles = [f for f in listOfFileNames if f.split('/')[0] == dir]
                        if len(listOfExtractFiles) > 0:
                            zip_ref.extractall(path=storage_path,members=listOfExtractFiles)
                            list = []
                            for file in os.listdir(os.path.join(storage_path,dir)):
                                f = open(os.path.join(storage_path,dir,file), encoding='utf-8')
                                list.append(json.load(f))
                                f.close()
                            setattr(self,dir,list)
                print(pbar)
                shutil.rmtree(storage_path)
            except:
                raise Exception('Error on zip file extracting !!')
        else:
            try:
                folders = [f for f in folders if f in os.listdir(zip_path)]
                pbar = pyprind.ProgBar(len(folders), title="Extracting json files from folder:")
                for dir in folders:
                    pbar.update(item_id = dir.ljust(15),force_flush=True)
                    list = []
                    for file in os.listdir(os.path.join(zip_path,dir)):
                        f = open(os.path.join(zip_path,dir,file), encoding='utf-8')
                        list.append(json.load(f))
                        f.close()
                    setattr(self,dir,list)
                print(pbar)
            except:
                raise Exception('Error on file extracting !!')



