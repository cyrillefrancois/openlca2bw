# -*- coding: utf-8 -*-
"""
Created on Fri Jul 16 14:01:02 2021

@author: cyrille.francois
"""
import json
import pyprind
import brightway2 as bw
from ..utils import return_attribute, get_item, uncertainty_convert, flattenNestedList
from .data import Json_database
from bw2io import normalize_units as normalize_unit

class Json_All(Json_database):
    def __init__(self):
        super().__init__()
        
        
    def json_elementary_flow(self):
        flows = self.flows
        flow_ref_unit = self.flow_unit
        ElemFlowList = []
        pbar = pyprind.ProgBar(len(flows), title="Extracting "+str(len(flows))+" flows from OpenLCA:")
        for f in flows:
            pbar.update(item_id = flows.index(f)+1)
            if return_attribute(f,'flowType') == 'ELEMENTARY_FLOW':
                catPath = return_attribute(f,('category','categoryPath'))
                if catPath is None:
                    type = return_attribute(f,('category','name'))
                    cat = (type,'unspecified')
                elif len(catPath) == 1:
                    type = catPath[0].lower()
                    cat = (type,return_attribute(f,('category','name')))
                elif catPath[1] == 'Resource':
                    type = 'natural resource'
                    cat = (type,return_attribute(f,('category','name')))
                elif catPath[1].split()[0] == 'Emission':
                    type = 'emission'
                    cat = (catPath[1].split()[-1],return_attribute(f,('category','name')))
                else:
                    type = catPath[1].lower()
                    cat = (type,return_attribute(f,('category','name')))
                ElemFlowList.append({
                    "categories": cat,
                    "code": f['@id'],
                    "CAS number": return_attribute(f,'cas'),
                    "database": "biosphere3",
                    "name": f['name'],
                    "type": type,
                    "unit": flow_ref_unit.loc[return_attribute(f['flowProperties'][0],('flowProperty','@id'))].values[0]
                })
        json_object = json.dumps(ElemFlowList, indent = 4)
        print(pbar)
        return(json_object) 
        
    def list_methods(self):
        methods = self.lcia_methods
        conv_units = self.unit_conv
        pbar = pyprind.ProgBar(len(methods), title="Extracting "+str(len(methods))+" LCIA methods from OpenLCA:")
        list_methods = []
        for m in methods:
            pbar.update(item_id = m['name'].ljust(30))
            impact_categories = m['impactCategories']
            for c in impact_categories:
                cate = get_item(self.lcia_categories,c['@id'])
                name = (m['name'],cate['name'])
                ref_unit = cate['referenceUnitName']
                list_cf = []
                impact_factors = cate['impactFactors']
                for i in impact_factors:
                    list_cf.append((
                        ('biosphere3',return_attribute(i,('flow','@id'))),
                        i['value'] / conv_units.loc[return_attribute(i,('unit','@id'))].values[0]
                      ))
                list_methods.append({
                    'name': name,
                    'ref_unit': ref_unit,
                    'list_cf': list_cf
                    })
        print(pbar)
        return(list_methods)
    
    # def U_equivalent(self, S_id=str):
    #     p_S = get_item(self.processes,S_id)
    #     if type(p_S['location']) is dict:
    #         p_S['location'] = self.location_table.loc[return_attribute(p_S,('location','@id'))].values[0] 
    #     for d in self.processes:
    #         if d['name'][:-2] == return_attribute(p_S,'name')[:-2] and d['name'][-2:] == ' U':
    #             if type(d['location']) is dict:
    #                 d['location'] = self.location_table.loc[return_attribute(d,('location','@id'))].values[0]    
    #             if return_attribute(d,'location') == return_attribute(p_S,'location'):
    #                 return d

    def U_equivalent(self, p_S):
        # p_S = get_item(self.processes,S_id)
        if return_attribute(p_S,'location') is None:
            p_S.update({'location': None})
        if type(p_S['location']) is dict:
            p_S['location'] = self.location_table.loc[return_attribute(p_S,('location','@id'))].values[0] 
        for d in self.processes:
            if d['name'][:-2] == return_attribute(p_S,'name')[:-2] and d['name'][-2:] == ' U':
                if return_attribute(d,'location') is None:
                    if return_attribute(p_S,'location') is None:
                        return d['@id'] 
                elif type(d['location']) is dict:
                    d['location'] = self.location_table.loc[return_attribute(d,('location','@id'))].values[0]    
                if return_attribute(d,'location') == return_attribute(p_S,'location'):
                    return d['@id'] 
    
    def keep_U_process(self, p): 
        if p['name'][-2:] != ' S':
            return True
        else: 
            for d in self.processes:
                if d['name'] == p['name'][:-2]+' U':
                    if type(d['location']) is dict:
                        d['location'] = self.location_table.loc[return_attribute(d,('location','@id'))].values[0]
                    if type(p['location']) is dict:
                        p['location'] = self.location_table.loc[return_attribute(p,('location','@id'))].values[0]
                    if return_attribute(d,'location') == return_attribute(p,'location'):
                        return False
        return True
    
    def list_nonuser_process(self, database_name = 'EcoInvent', nonuser_folders = [],exclude_S = False):
        processes = self.processes
        conv_loc = self.location_table
        list_process = []
        list_process_parameters = []
        list_missed_providers = []
        pbar = pyprind.ProgBar(len(processes), title="Treatment of openlca processes, "+str(len(processes))+" processes extracted from OpenLCA:")
        for p in processes:
            pbar.update(item_id = processes.index(p)+1)
            if exclude_S:
                if self.keep_U_process(p) == False:
                    continue
            if return_attribute(p,('category','categoryPath')) is None:
                continue
            if return_attribute(p,('category','categoryPath'))[0] in nonuser_folders:
                classif = [return_attribute(p, ('category','name'))]
                classif.insert(0,return_attribute(p, ('category','categoryPath')))
                exch = return_attribute(p,'exchanges')
                ref_exch = [exc for exc in exch if return_attribute(exc,'quantitativeReference')][0]
                if return_attribute(ref_exch,('flow','flowType')) == 'WASTE_FLOW':
                    cf_waste = -1
                else:
                    cf_waste = 1
                p_arg = {'comment': return_attribute(p, 'description'),
                     'classification': classif,
                     'activity': return_attribute(p,'@id'),
                     'name': return_attribute(p,'name'),
                     'parameters': return_attribute(p,'parameters'),
                     'authors': return_attribute(p,('processDocumentation','dataGenerator','name')),
                     'type': 'process',
                     'code': return_attribute(p,'@id'),
                     'reference product': return_attribute(ref_exch,('flow','name')),
                     'flow': return_attribute(ref_exch,('flow','@id')),
                     'unit': normalize_unit(return_attribute(ref_exch,('unit','name'))),
                     'unit_id': return_attribute(ref_exch,('unit','@id')),
                     'production amount': return_attribute(ref_exch,'amount') * cf_waste
                     }
                if return_attribute(p,('location','@id')) is not None:
                    p_arg.update({'location': conv_loc.loc[return_attribute(p,('location','@id'))].values[0]})
                list_exc = []
                for exc in exch:
                    cf_IO = 1
                    if return_attribute(exc,('flow','flowType')) == 'WASTE_FLOW':
                        if return_attribute(exc,'input') == False:
                            cf_IO = -1
                        else:
                            if return_attribute(exc,'quantitativeReference'):
                                cf_IO = -1
                            elif return_attribute(exc,'avoidedProduct') == False:
                                print("Coproduct "+str(return_attribute(exc,('flow','name')))+" related to process "+str(return_attribute(p,'name'))+"unspecified avoided\nExchange not extracted in brightway !!!")
                                continue
                    if return_attribute(exc,('flow','flowType')) == 'ELEMENTARY_FLOW':
                        if return_attribute(exc,'input') == True and return_attribute(exc,('flow','categoryPath'))[1] != 'Resource':
                            cf_IO = -1
                    if return_attribute(exc,('flow','flowType')) == 'PRODUCT_FLOW' and return_attribute(exc,'input') == False:
                        if return_attribute(exc,'quantitativeReference'):
                            cf_IO = 1
                        elif return_attribute(exc,'avoidedProduct'):
                            cf_IO = -1
                        else:
                            print("Coproduct "+str(return_attribute(exc,('flow','name')))+" related to process "+str(return_attribute(p,'name'))+"unspecified avoided\nExchange not extracted in brightway !!!")
                            continue
                    exc_arg = {
                        'flow': return_attribute(exc,('flow','@id')),
                        'name': return_attribute(exc,('flow','name')),
                        'unit': normalize_unit(return_attribute(exc,('unit','name'))),
                        'unit_id': return_attribute(exc,('unit','@id')),
                        'comment': return_attribute(exc,'descrition'),
                        'formula': return_attribute(exc,'amountFormula'),
                        'amount': return_attribute(exc,'amount') * cf_IO
                        }
                    if return_attribute(exc,'quantitativeReference'):
                        exc_arg.update({'type': 'production',
                                        'input': (database_name,return_attribute(p,'@id'))
                                        })
                    elif return_attribute(exc,('flow','flowType')) == 'ELEMENTARY_FLOW':
                        exc_arg.update({'type': 'biosphere',
                                        'input': ('biosphere3',return_attribute(exc,('flow','@id')))
                                        })
                    else:
                        if  return_attribute(exc,'defaultProvider') is None:
                            list_missed_providers.append({'activity':(database_name,return_attribute(p,'@id')),
                                                          'num exchange': return_attribute(exc,'internalId'),
                                                          'flow': return_attribute(exc,('flow','@id'))})
                            exc_arg.update({'type': 'technosphere',
                                            'input': (database_name,return_attribute(p,'@id'))
                                            })
                        else:
                            if exclude_S and not self.keep_U_process(return_attribute(exc,'defaultProvider')):
                                id_U = self.U_equivalent(return_attribute(exc,('defaultProvider','@id')))
                                exc_arg.update({'type': 'technosphere',
                                                'input': (database_name,id_U),
                                                'activity': id_U
                                                })
                            else:
                                exc_arg.update({'type': 'technosphere',
                                                'input': (database_name,return_attribute(exc,('defaultProvider','@id'))),
                                                'activity': return_attribute(exc,('defaultProvider','@id'))
                                                })
                if return_attribute(exc,'dqEntry') is not None:
                    exc_arg.update({'pedigree': {
                        'reliability': return_attribute(exc,('dqEntry',1)),
                        'completeness': return_attribute(exc,('dqEntry',3)),
                        'temporal correlation': return_attribute(exc,('dqEntry',5)),
                        'geographical correlation': return_attribute(exc,('dqEntry',7)),
                        'further technological correlation': return_attribute(exc,('dqEntry',9))}
                    })
                if return_attribute(exc,'uncertainty') is not None:
                    uncertainties = uncertainty_convert(return_attribute(exc,'uncertainty'))
                    if uncertainties is not None:
                        exc_arg.update(uncertainty_convert(return_attribute(exc,'uncertainty')))
                exc_arg = {k: v for k, v in exc_arg.items() if v}
                list_exc.append(exc_arg)
            p_arg.update({'exchanges': list_exc}) 
            list_process.append(p_arg)
            if return_attribute(p,'parameters') is not None:
                list_process_parameters.append(((database_name,return_attribute(p,'@id')), [param['@id'] for param in return_attribute(p,'parameters')]))
        print(pbar)
        print(str(len(list_process))+" nonuser processes find and read\n")
        return list_process, list_process_parameters, list_missed_providers
    
    def extract_list_process(self, databases_names, dict_list_id={}, exclude_S=False,update=False):
        processes = self.processes
        conv_loc = self.location_table
        db_names = list(databases_names.keys())
        db_data = []
        db_list_id = []
        for db in db_names:
            list_process_id = []
            for p in processes:
                if return_attribute(p,'category') is None:
                    if None not in flattenNestedList(databases_names[db]):
                        continue
                else:
                    if return_attribute(p,('category','categoryPath')) is None:
                        if return_attribute(p,('category','name')) not in flattenNestedList(databases_names[db]):
                            continue
                    else:
                        if return_attribute(p,('category','categoryPath'))[0] not in flattenNestedList(databases_names[db]):
                            continue
                list_process_id.append(return_attribute(p,'@id'))
            db_list_id.append(list_process_id)
        db_list_id = dict(zip(db_names,db_list_id))
        list_process_parameters = []
        list_missed_providers = []
        if update:
            for db in bw.databases:
                if db != 'biosphere3' and db not in db_names:
                    self.processes.extend([{'@id': act['code'],'name': act['name'],'location': return_attribute(act,'location')} for act in bw.Database(db)])
        for db in db_names:
            pbar = pyprind.ProgBar(len(db_list_id[db]), title="Extracting "+str(len(db_list_id[db]))+" processes from OpenLCA for "+str(db)+" database:")    
            list_process = []
            for p in processes:
                if p['@id'] not in db_list_id[db]:
                    continue
                pbar.update(item_id = len(list_process)+1)
                if return_attribute(p,'exchanges') is None:
                    print(p)
                classif = [return_attribute(p, ('category','name'))]
                classif.insert(0,return_attribute(p, ('category','categoryPath')))
                exch = return_attribute(p,'exchanges')
                ref_exch = [exc for exc in exch if return_attribute(exc,'quantitativeReference')][0]
                p_arg = {'comment': return_attribute(p, 'description'),
                         'classification': classif,
                         'activity': return_attribute(p,'@id'),
                         'name': return_attribute(p,'name'),
                         'parameters': return_attribute(p,'parameters'),
                         'authors': return_attribute(p,('processDocumentation','dataGenerator','name')),
                         'type': 'process',
                         'code': return_attribute(p,'@id'),
                         'reference product': return_attribute(ref_exch,('flow','name')),
                         'flow': return_attribute(ref_exch,('flow','@id')),
                         'unit': normalize_unit(return_attribute(ref_exch,('unit','name'))),
                         'unit_id': return_attribute(ref_exch,('unit','@id')),
                         'production amount': return_attribute(ref_exch,'amount')
                         }
                if return_attribute(p,('location','@id')) is not None:
                    p_arg.update({'location': conv_loc.loc[return_attribute(p,('location','@id'))].values[0]})
                list_exc = []
                for exc in exch:
                    cf_IO = 1
                    if return_attribute(exc,('flow','flowType')) == 'WASTE_FLOW':
                        if return_attribute(exc,'input') == False:
                            cf_IO = -1
                        else:
                            if return_attribute(exc,'quantitativeReference'):
                               cf_IO = -1
                            elif return_attribute(exc,'avoidedProduct') == False:
                                print("Coproduct "+str(return_attribute(exc,('flow','name')))+" related to process "+str(return_attribute(p,'name'))+"unspecified avoided\nExchange not extracted in brightway !!!")
                                continue
                    if return_attribute(exc,('flow','flowType')) == 'ELEMENTARY_FLOW':
                        if return_attribute(exc,'input') == False and return_attribute(exc,('flow','categoryPath'))[1] == 'Resource':
                            cf_IO = -1
                        if return_attribute(exc,'input') == True and return_attribute(exc,('flow','categoryPath'))[1] != 'Resource':
                            cf_IO = -1
                    if return_attribute(exc,('flow','flowType')) == 'PRODUCT_FLOW' and return_attribute(exc,'input') == False:
                        if return_attribute(exc,'quantitativeReference'):
                            cf_IO = 1
                        elif return_attribute(exc,'avoidedProduct'):
                            cf_IO = -1
                        else:
                            print("Coproduct "+str(return_attribute(exc,('flow','name')))+" related to process "+str(return_attribute(p,'name'))+"unspecified avoided\nExchange not extracted in brightway !!!")
                            continue
                    exc_arg = {
                        'flow': return_attribute(exc,('flow','@id')),
                        'name': return_attribute(exc,('flow','name')),
                        'unit': normalize_unit(return_attribute(exc,('unit','name'))),
                        'unit_id': return_attribute(exc,('unit','@id')),
                        'comment': return_attribute(exc,'descrition'),
                        'formula': return_attribute(exc,'amountFormula'),
                        'amount': return_attribute(exc,'amount') * cf_IO
                        }
                    if return_attribute(exc,'quantitativeReference'):
                        exc_arg.update({'type': 'production',
                                        'input': (db,return_attribute(p,'@id'))
                                        })
                    elif return_attribute(exc,('flow','flowType')) == 'ELEMENTARY_FLOW':
                        exc_arg.update({'type': 'biosphere',
                                        'input': ('biosphere3',return_attribute(exc,('flow','@id')))
                                        })
                    else:
                        if  return_attribute(exc,'defaultProvider') is None:
                            list_missed_providers.append({'activity':(db,return_attribute(p,'@id')),
                                                          'num exchange': return_attribute(exc,'internalId'),
                                                          'flow': return_attribute(exc,('flow','@id'))})
                            exc_arg.update({'type': 'technosphere',
                                            'input': (db,return_attribute(p,'@id'))
                                            })
                        else:
                            if exclude_S and self.keep_U_process(return_attribute(exc,'defaultProvider')) == False:
                                # id_provider = self.U_equivalent(return_attribute(exc,('defaultProvider','@id')))
                                id_provider = self.U_equivalent(return_attribute(exc,'defaultProvider'))
                            else:
                                id_provider = return_attribute(exc,('defaultProvider','@id'))
                            exc_arg.update({'type': 'technosphere',
                                            'activity': id_provider
                                            })
                            if id_provider in flattenNestedList([v for v  in dict_list_id.values()]):
                                exc_arg.update({'input': ([k for k, v in dict_list_id.items() if id_provider in v][0],id_provider)})
                            else:
                                exc_arg.update({'input': ([name for name in list(db_list_id.keys()) if id_provider in db_list_id[name]][0],id_provider)})
                    if return_attribute(exc,'dqEntry') is not None:
                        exc_arg.update({'pedigree': {
                            'reliability': return_attribute(exc,('dqEntry',1)),
                            'completeness': return_attribute(exc,('dqEntry',3)),
                            'temporal correlation': return_attribute(exc,('dqEntry',5)),
                            'geographical correlation': return_attribute(exc,('dqEntry',7)),
                            'further technological correlation': return_attribute(exc,('dqEntry',9))}
                        })
                    if return_attribute(exc,'uncertainty') is not None:
                        uncertainties = uncertainty_convert(return_attribute(exc,'uncertainty'))
                        if uncertainties is not None:
                            exc_arg.update(uncertainty_convert(return_attribute(exc,'uncertainty')))
                    exc_arg = {k: v for k, v in exc_arg.items() if v}
                    list_exc.append(exc_arg)
                p_arg.update({'exchanges': list_exc}) 
                list_process.append(p_arg)
                if return_attribute(p,'parameters') is not None:
                    self.parameters.extend(return_attribute(p,'parameters'))
                    list_process_parameters.append(((db,return_attribute(p,'@id')), [p['@id'] for p in return_attribute(p,'parameters')]))
            db_data.append(list_process)
        print(pbar)
        return dict(zip(db_names, db_data)), list_process_parameters, list_missed_providers    