# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 22:11:22 2021

@author: cyrille.francois
"""
import olca
from .extract import Extraction_functions

class IPC_Extractor(olca.Client, Extraction_functions):
    def __init__(self, port: int = 8080):
        super().__init__(port)
        self.flow_unit = None
        self.unit_conv = None
        self.location_table = None
        self.change_param = {}
        self.convert_ids = {}
        
 
