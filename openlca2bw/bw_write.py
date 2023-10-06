# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 22:26:44 2021

@author: cyrille.francois
"""
import json
import types
import brightway2 as bw
from .utils import return_attribute, flattenNestedList, main_flow_table, change_formula, uncertainty_convert, change_param_names, rescale_exchange
from bw2io.importers.base_lci import LCIImporter
from bw2data.parameters import ActivityParameter
from bw2io.strategies import normalize_units, drop_unspecified_subcategories
from pathlib import Path

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

def import_parameters(all_parameters,list_process_parameters,change_param):
    if isinstance(all_parameters, types.GeneratorType): 
        all_parameters = [p.to_json() for p in all_parameters]
    global_parameters = []
    process_parameters = []
    for p in all_parameters:
        if return_attribute(p,'parameterScope') == 'GLOBAL_SCOPE':
            g_dict = {}
            g_dict['amount'] = return_attribute(p,'value')
            g_dict['id'] = return_attribute(p,'@id')
            g_dict['comment'] = return_attribute(p,'description')
            g_dict['name'] = change_formula(return_attribute(p,'name'),change_param)
            g_dict['formula'] = change_formula(return_attribute(p,'formula'),change_param)
            if uncertainty_convert(return_attribute(p,'uncertainty')) is not None:
                g_dict.update(uncertainty_convert(return_attribute(p,'uncertainty')))
            global_parameters.append(g_dict)
        if return_attribute(p,'parameterScope') == 'PROCESS_SCOPE':
            process_parameters.append(p)
    bw.parameters.new_project_parameters(global_parameters)
    for process, params in list_process_parameters.items():
        if params == []:
            continue
        p_dict = []
        p_changes = change_param.copy()
        p_params = [par for par in process_parameters if return_attribute(par,'@id') in params]
        if len(p_params)==0:
            continue
        p_changes.update(change_param_names([return_attribute(par,'name') for par in p_params])) #I don't consider the possibility that exchange formula use parameters with same name related to global and process parameters (possible in OLCA but who do that !!) 
        act = bw.get_activity(process)
        for exc in act.exchanges():
            if exc.get('formula',None):
                exc['formula'] = change_formula(exc['formula'],p_changes)
                exc.save()
        for param in p_params:
            i_dict = {'name': change_formula(return_attribute(param,'name'),p_changes),
                      'database': process[0],
                      'code': process[1],
                      'amount': return_attribute(param,'value'),
                      'formula': change_formula(return_attribute(param,'formula'),p_changes),
                      'comment': return_attribute(param,'description'),
                      'id': return_attribute(param,'@id')
                      }
            if uncertainty_convert(return_attribute(param,'uncertainty')) is not None:
                i_dict.update(uncertainty_convert(return_attribute(param,'uncertainty')))
            p_dict.append(i_dict)
        bw.parameters.new_activity_parameters(p_dict, process[1])
        bw.parameters.add_exchanges_to_group(process[1], process)
        ActivityParameter.recalculate_exchanges(process[1])        

def check_elementary_exchanges(dict_processes):
    print("Checking for wrong elementary flows ids\n")
    i = 0
    with open(Path(__file__).parent.resolve()/"change_elem_flows.json") as json_file:
        flows_ids_dict = json.load(json_file)
    for db in dict_processes.keys():
        for act in dict_processes[db]:
            for exc in act['exchanges']:
                if exc['flow'] in flows_ids_dict.keys():
                    i+=1
                    exc['flow'] = flows_ids_dict[exc['flow']]
                    exc['input'] = (exc['input'][0], exc['flow'])
                    
    print(str(i)+" exchanges modified to match correct elementary flow")
    return dict_processes
    
def check_exchanges_units(conv_factors, databases_names):
    print("Units checking for activities exchanges\n")
    i = 0
    for db in flattenNestedList(databases_names):
        for act in bw.Database(db):
            for exc in act.exchanges():
                try:
                    if exc.unit != exc['unit']:
                        i += 1
                        f0 = conv_factors[conv_factors.unit_name == exc['unit']].conv_factor.values[0]
                        f1 = conv_factors[conv_factors.unit_name == exc.unit].conv_factor.values[0]
                        cf_units = f0 / f1
                        exc['unit'] = exc.unit
                        rescale_exchange(exc,cf_units)
                        exc.save() 
                except:
                    print("Error with activity "+str(act)+" exchange with '"+str(exc['name'])+"' is missing (check provider)\n")
                    exc.delete()
                    print("Exchange deleted !!!")
    print(str(i)+" exchanges modified to match units")
    

def single_provider_retriver(list_missed_providers, verbose=False):
    if len(list_missed_providers) == 0:
        return
    print("Research of "+str(len(list_missed_providers))+" not defined providers\n")
    nb_remove = 0
    activity_main_flow = main_flow_table()
    for exc in list_missed_providers:
        try:
            act = bw.get_activity(exc['activity'])
        except:
            continue
        ini_exc = [e for e in list(act.exchanges()) if e['flow'] == exc['flow']][0]
        list_providers = activity_main_flow[activity_main_flow.flow == exc['flow']]
        if len(list_providers) == 0:
            if verbose:
                print('No provider find for activity '+str(act)+' flow '+exc['flow']+'\nExchange deleted !!!')
            ini_exc.delete()
            nb_remove += 1
            continue
        elif len(list_providers) > 1:
            if verbose:
                print('More than one provider find for activity '+str(act)+' flow '+exc['flow']+'\nExchange deleted !!!')
            ini_exc.delete()
            nb_remove += 1 
            continue
        else:
            ini_exc.delete() 
            new_dict = ini_exc.as_dict()
            new_dict['input'] = (list_providers['database'].values[0],list_providers['code'].values[0])
            new_dict.pop('output')
            new_exc = act.new_exchange(**new_dict)
            new_exc.save()
    
    print(str(nb_remove)+" exchanges deleted due to no provider or to many providers available\n")
    return
                       


