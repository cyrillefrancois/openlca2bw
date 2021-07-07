# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 22:26:44 2021

@author: cyrille.francois
"""
import json
import brightway2 as bw
import olca
from .utils import return_attribute, flattenNestedList
from bw2io.importers.base_lci import LCIImporter
from bw2data.parameters import ActivityParameter
from bw2io.strategies import normalize_units, drop_unspecified_subcategories

class OpenLCABiosphereImporter(LCIImporter):
    format = 'Ecoinvent XML'

    def __init__(self, name="biosphere3",jsonfile='{}'):
        self.db_name = name
        self.data = self.extract(jsonfile)
        self.strategies = [
            normalize_units,
            drop_unspecified_subcategories,
        ]

    def extract(self,jsonfile):
        return json.loads(jsonfile)

def create_OpenLCA_biosphere3(json_biosphere,overwrite=False):
    # from .importers import OpenLCABiosphereImporter
    eb = OpenLCABiosphereImporter(jsonfile=json_biosphere)
    eb.apply_strategies()
    eb.write_database(overwrite=overwrite)

def register_method(methodName, methodUnit, method_data):
    meth = bw.Method(methodName)
    if methodName in list(bw.methods):
        meth.deregister()
    meth.register(unit=methodUnit)
    meth.write(method_data)
    meth.process()

def create_OpenLCA_LCIAmethods(self):
    methods = self.list_methods()
    for m in methods:
        register_method(methodName=m['name'],methodUnit=m['ref_unit'],method_data=m['list_cf'])

def import_parameters(self,list_process_parameters):
    parameters = self.get_all(olca.Parameter)
    global_parameters = []
    process_parameters = []
    for p in parameters:
        if p.parameter_scope.name == 'GLOBAL_SCOPE':
            g_dict = {k: v for k, v in p.to_json().items() if k in ['name','formula','value']}
            g_dict['amount'] = g_dict.pop('value')
            global_parameters.append(g_dict)
        if p.parameter_scope.name == 'PROCESS_SCOPE':
            process_parameters.append(p.to_json())
    bw.parameters.new_project_parameters(global_parameters)
    for p in list_process_parameters:
        p_dict = []
        for i in flattenNestedList(p[1]):
            param = [par for par in process_parameters if par['@id'] == i][0]
            i_dict = {'name': return_attribute(param,'name'),
                      'database': p[0][0],
                      'code': p[0][1],
                      'amount': return_attribute(param,'value'),
                      'formula': return_attribute(param,'formula')}
            p_dict.append(i_dict)
        bw.parameters.new_activity_parameters(p_dict, p[0][1])
        bw.parameters.add_exchanges_to_group(p[0][1], p[0])
        ActivityParameter.recalculate_exchanges(p[0][1])
       
def check_exchanges_units(self, databases_names):
    print("Units checking for activities exchanges\n")
    conv_factors = self.unit_conv
    i = 0
    for db in flattenNestedList(databases_names):
        for act in bw.Database(db):
            for exc in act.exchanges():
                if exc.unit != exc['unit']:
                    i += 1
                    f0 = conv_factors[conv_factors.unit_name == exc['unit']].conv_factor.values[0]
                    f1 = conv_factors[conv_factors.unit_name == exc.unit].conv_factor.values[0]
                    cf_units = f0 / f1
                    exc['unit'] = exc.unit
                    exc['amount'] = exc['amount'] * cf_units
                    if 'formula' in exc:
                        exc['formula'] = exc['formula']+" * "+str(cf_units)
                    exc.save()    
    print(str(i)+" exchanges modified to match units")
    



