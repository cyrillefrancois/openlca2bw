# -*- coding: utf-8 -*-
"""
Created on Fri Jul 16 10:06:05 2021

@author: cyrille.francois
"""

import zipfile
import os
import json
import pyprind
import shutil
import pandas as pd
from ..utils import return_attribute, get_item
from bw2io import normalize_units as normalize_unit


class Json_database():
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


    def extract_zip_openlca(self,zip_path=str,storage_path=os.getcwd(),
                            folders=['categories','unit_groups','flow_properties','locations','flows','lcia_categories','lcia_methods','processes','parameters']):
        storage_path = os.path.join(storage_path,'olca2bw_Unzip')
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
    

    
    def flow_properties_unit(self):
        flow_prop = self.flow_properties
        list_flow_prop = pd.DataFrame(columns=['flow_prop_id','ref_unit'])
        for f in flow_prop:
            unit_group = get_item(self.unit_groups,return_attribute(f,('unitGroup','@id')))
            for u in unit_group['units']:
                if return_attribute(u,'referenceUnit'):
                    list_flow_prop = list_flow_prop.append({'flow_prop_id': f['@id'],'ref_unit': u['name']},ignore_index=True)
        list_flow_prop = list_flow_prop.set_index('flow_prop_id') 
        self.flow_unit = list_flow_prop
    
    
    def location_convert(self):
        locations = self.locations
        list_locations = pd.DataFrame(columns=['location_id','location_code'])
        for l in locations:
                list_locations = list_locations.append({'location_id': l['@id'],'location_code': l['code']},ignore_index=True)
        list_locations = list_locations.set_index('location_id') 
        self.location_table = list_locations
    
    def unit_convert_factor(self):
        units = self.unit_groups
        flow_ref_unit = self.flow_unit
        list_units = pd.DataFrame(columns=['unit_id','conv_factor','unit_name','ref_unit'])
        for u_group in units:
            for u in u_group['units']:
                list_units = list_units.append({
                    'unit_id': u['@id'], 
                    'conv_factor': u['conversionFactor'],
                    'unit_name': normalize_unit(u['name']),
                    'ref_unit': normalize_unit(flow_ref_unit.loc[return_attribute(u_group,('defaultFlowProperty','@id'))].values[0])},ignore_index=True)
        list_units = list_units.set_index('unit_id') 
        self.unit_conv = list_units
            