# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 22:11:22 2021

@author: cyrille.francois
"""
from importlib.metadata import version
if version("olca_ipc") == "0.0.12":
    import olca
else:
    import olca_schema as olca
    import olca_ipc as ipc
    olca.Client = ipc.Client

from .extract import Extraction_functions

class IPC_Extractor():
    def __init__(self, port: int = 8080,olca_module = olca):  
        olca_module.Client.__init__(self,port)
        self.__class__ = type(self.__class__.__name__,
                              (olca_module.Client, Extraction_functions),
                              dict(self.__class__.__dict__))
        self.olca_module = olca_module
        self.flow_unit = None
        self.unit_conv = None
        self.location_table = None
        self.change_param = {}
        self.convert_ids = {}
        
 
