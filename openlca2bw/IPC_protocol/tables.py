# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 22:11:22 2021

@author: cyrille.francois
"""
import pandas as pd
import olca
from bw2io import normalize_units as normalize_unit

class ClientData(olca.Client):
    def __init__(self, port: int = 8080):
        super().__init__(port)
        self.flow_unit = self.flow_properties_unit()
        self.unit_conv = self.unit_convert_factor()
        self.location_table = self.location_convert()

    def flow_properties_unit(self):
        flow_prop = self.get_all(olca.FlowProperty)
        list_flow_prop = pd.DataFrame(columns=['flow_prop_id','ref_unit'])
        for f in flow_prop:
            unit_group = self.get(olca.UnitGroup,model_id=f.unit_group.id)
            for u in unit_group.units:
                if u.reference_unit:
                    list_flow_prop = list_flow_prop.append({'flow_prop_id': f.id,'ref_unit': u.name},ignore_index=True)
        list_flow_prop = list_flow_prop.set_index('flow_prop_id') 
        return(list_flow_prop)
    
    def location_convert(self):
        locations = self.get_all(olca.Location)
        list_locations = pd.DataFrame(columns=['location_id','location_code'])
        for l in locations:
                list_locations = list_locations.append({'location_id': l.id,'location_code': l.code},ignore_index=True)
        list_locations = list_locations.set_index('location_id') 
        return(list_locations)
    
    def unit_convert_factor(self):
        units = self.get_all(olca.UnitGroup)
        flow_ref_unit = self.flow_unit
        list_units = pd.DataFrame(columns=['unit_id','conv_factor','unit_name','ref_unit'])
        for u_group in units:
            for u in u_group.units:
                list_units = list_units.append({
                    'unit_id': u.id, 
                    'conv_factor': u.conversion_factor,
                    'unit_name': normalize_unit(u.name),
                    'ref_unit': normalize_unit(flow_ref_unit.loc[u_group.default_flow_property.id].values[0])},ignore_index=True)
        list_units = list_units.set_index('unit_id') 
        return(list_units)

