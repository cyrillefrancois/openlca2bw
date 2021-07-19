# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 22:26:44 2021

@author: cyrille.francois
"""
import json
import brightway2 as bw
from .utils import return_attribute, flattenNestedList, main_flow_table
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

def create_OpenLCA_LCIAmethods(methods):
    for m in methods:
        register_method(methodName=m['name'],methodUnit=m['ref_unit'],method_data=m['list_cf'])

def import_parameters_ipc(all_parameters,list_process_parameters):
    global_parameters = []
    process_parameters = []
    for p in all_parameters:
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

def import_parameters_json(all_parameters,list_process_parameters):
    global_parameters = []
    process_parameters = []
    for p in all_parameters:
        if return_attribute(p,'parameterScope') == 'GLOBAL_SCOPE':
            g_dict = {k: v for k, v in p.items() if k in ['name','formula','value']}
            g_dict['amount'] = g_dict.pop('value')
            global_parameters.append(g_dict)
        if return_attribute(p,'parameterScope') == 'PROCESS_SCOPE':
            process_parameters.append(p)
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

def check_exchanges_units(conv_factors, databases_names):
    print("Units checking for activities exchanges\n")
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
    

def single_provider_retriver(list_missed_providers):
    if len(list_missed_providers) == 0:
        return
    print("Reseach of not defined providers\n")
    activity_main_flow = main_flow_table()
    for exc in list_missed_providers:
        try:
            act = bw.get_activity(exc['activity'])
        except:
            continue
        ini_exc = [e for e in list(act.exchanges()) if e['flow'] == exc['flow']][0]
        list_providers = activity_main_flow[activity_main_flow.flow == exc['flow']]
        if len(list_providers) == 0:
            print('No provider find for activity '+str(act)+' flow '+exc['flow']+'\nActivity deleted !!!')
            act.delete()
            continue
        elif len(list_providers) > 1:
            print('More than one provider find for activity '+str(act)+' flow '+exc['flow']+'\nActivity deleted !!!')
            act.delete()
            continue
        else:
            ini_exc.delete() 
            new_dict = ini_exc.as_dict()
            new_dict['input'] = (list_providers['database'].values[0],list_providers['code'].values[0])
            new_dict.pop('output')
            new_exc = act.new_exchange(**new_dict)
            new_exc.save()
                       


