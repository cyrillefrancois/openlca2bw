from importlib.metadata import version
if version("olca_ipc") == "0.0.12":
    import olca
else:
    import olca_schema as olca
    import olca_ipc as ipc
    olca.Client = ipc.Client

from .allocation import convert_alloc_factor
import pyprind
from .utils import flattenNestedList, return_attribute, ref_flow, root_folder, is_product, uncertainty_convert, convert_to_internal_ids, normalize_units
import brightway2 as bw
#from bw2io.units import normalize_units
import json
import pandas as pd

class Extraction_functions():

    def flow_properties_unit(self):
        try:
            #flow_prop = self.get_all(olca.FlowProperty)
            flow_prop = self.get_all(self.olca_module.FlowProperty)
        except:
            flow_prop = self.flow_properties
        list_flow_prop = pd.DataFrame(columns=['flow_prop_id','ref_unit'])
        for f in flow_prop:
            try:
                unit_group = [u_group for u_group in self.unit_groups if u_group["@id"] == return_attribute(f,('unitGroup','@id'))][0]
            except:
                #unit_group = self.get(olca.UnitGroup,uid=f.unit_group.id)
                unit_group = self.get(self.olca_module.UnitGroup,uid=f.unit_group.id)
            for u in return_attribute(unit_group,'units'):
                if return_attribute(u,'referenceUnit') or return_attribute(u,'isRefUnit'):
                    list_flow_prop = pd.concat([list_flow_prop,pd.DataFrame.from_records([{'flow_prop_id': return_attribute(f,'@id'),'ref_unit': return_attribute(u,'name')}])], ignore_index=True)
        list_flow_prop = list_flow_prop.drop_duplicates()
        list_flow_prop = list_flow_prop.set_index('flow_prop_id') 
        return(list_flow_prop)

    def location_convert(self):
        try:
            #locations = self.get_all(olca.Location)
            locations = self.get_all(self.olca_module.Location)
        except:
            locations = self.locations
        list_locations = pd.DataFrame(columns=['location_id','location_code'])
        for l in locations:
                list_locations = pd.concat([list_locations,pd.DataFrame.from_records([{'location_id': return_attribute(l,'@id'),'location_code': return_attribute(l,'code')}])],ignore_index=True)
        list_locations = list_locations.drop_duplicates()
        list_locations = list_locations.set_index('location_id') 
        return(list_locations)

    def unit_convert_factor(self):
        try:
            #units = self.get_all(olca.UnitGroup)
            units = self.get_all(self.olca_module.UnitGroup)
        except:
            units = self.unit_groups
        flow_ref_unit = self.flow_unit
        list_units = pd.DataFrame(columns=['unit_id','conv_factor','unit_name','ref_unit'])
        for u_group in units:
            for u in return_attribute(u_group,'units'):
                if  return_attribute(u_group,'defaultFlowProperty'):
                    ref_u = normalize_units(flow_ref_unit.loc[return_attribute(u_group,('defaultFlowProperty','@id'))].values[0])
                else:
                    ref_u =  normalize_units([return_attribute(uni,'name') for uni in return_attribute(u_group,'units') if (return_attribute(uni,'referenceUnit') or return_attribute(uni,'isRefUnit'))][0]) 
                list_units = pd.concat([list_units,pd.DataFrame.from_records([{
                    'unit_id': return_attribute(u,'@id'), 
                    'conv_factor': return_attribute(u,'conversionFactor'),
                    'unit_name': normalize_units(return_attribute(u,'name')),
                    'ref_unit': ref_u}])],ignore_index=True)
        list_units = list_units.drop_duplicates()
        list_units = list_units.set_index('unit_id') 
        return(list_units)

    def params_providers(self,process):
        process_flow = None
        process_name = return_attribute(process,'name')
        process_id = return_attribute(process,'@id')
        if len(return_attribute(process,'name').split(' | ')) >1:
            process_split = return_attribute(process,'name').split(' | ')
            process_name = process_split[0]
            process_flow = process_split[1]
        #if isinstance(process, olca.schema.Ref) and not process_flow:
        #    process = self.get(olca.Process,uid=process_id)
        if (getattr(self,"olca_module",False) and isinstance(process, self.olca_module.schema.Ref)) and not process_flow:
            process = self.get(self.olca_module.Process,uid=process_id)
            process_flow = ref_flow(process, name = True)
        process_loc = return_attribute(process,'location')
        return {'id': process_id, 'name': process_name, 'flow': process_flow, 'location': process_loc}


    def change_S_process(self, process):
        try:
            #processes = self.get_descriptors(olca.Process)
            processes = self.get_descriptors(self.olca_module.Process)
        except:
            processes = self.processes  
        if return_attribute(process,'processType') == 'UNIT_PROCESS':
            return (True,'')
        else:
            process_id = return_attribute(process,'@id')
            if len(return_attribute(process,'name').split(' | ')) >1:
                process_split = return_attribute(process,'name').split(' | ')
                process_name = process_split[0]
                process_flow = process_split[1]
            else:
                #if isinstance(process, olca.schema.Ref):
                #    process = self.get(olca.Process,uid=process_id)
                if getattr(self,"olca_module",False) and isinstance(process, self.olca_module.schema.Ref):
                    process = self.get(self.olca_module.Process,uid=process_id)
                process_name = return_attribute(process,'name')
                process_flow = ref_flow(process)
            process_loc = return_attribute(process,'location')
            if type(process_loc) is not str and process_loc is not None:
                process_loc = self.location_table.loc[return_attribute(process,('location','@id'))].values[0]
            for p in processes:
                p_flow = None
                if len(return_attribute(p,'name').split(' | ')) >1:
                    p_split = return_attribute(p,'name').split(' | ')
                    p_name = p_split[0]
                    p_flow = p_split[1]
                else:
                    p_name = return_attribute(p,'name')
                p_loc = return_attribute(p,'location')
                if type(p_loc) is not str and p_loc is not None:
                    p = self.location_table.loc[return_attribute(p,('location','@id'))].values[0]
                if p_name == process_name and p_loc == process_loc:
                    if not p_flow:
                        try:
                            p_flow = ref_flow(p)
                        except:
                            #p = self.get(olca.Process,uid=return_attribute(p,'@id'))
                            p = self.get(self.olca_module.Process,uid=return_attribute(p,'@id'))
                            p_flow = ref_flow(p)
                    if p_flow == process_flow:
                        return (False,return_attribute(p,'@id'))
            return (True,'') 


    def json_elementary_flow(self):
        try:
            #flows = self.get_all(olca.Flow)
            flows = self.get_all(self.olca_module.Flow)
        except:
            flows = self.flows
        flows = [f for f in flows if return_attribute(f,'flowType') == 'ELEMENTARY_FLOW']
        ElemFlowList = []
        pbar = pyprind.ProgBar(len(flows), title="Extracting "+str(len(flows))+" flows from OpenLCA:")
        for f in flows:
            pbar.update()
            if return_attribute(f,'category') is None:
                type = 'unspecified'
                cat = (type,'unspecified')
            else:
                catPath = return_attribute(f,('category','categoryPath'))
                if catPath is None:
                    if return_attribute(f,'category') is str:
                        catPath = return_attribute(f,'category').split("/")
                try:
                    catPath.remove('Elementary flows')
                except:
                    pass
                if catPath is None or catPath == []:
                    type = return_attribute(f,('category','name'))
                    if type is None:
                        type = return_attribute(f,'category').split("/")[-1]
                    cat = (type,'unspecified')
                elif catPath[0] == 'Resource':
                    type = 'natural resource'
                    cat = (type,return_attribute(f,('category','name')))
                    if cat is None:
                        cat = catPath[-1]
                elif catPath[0].split()[0] == 'Emission':
                    type = 'emission'
                    cat = (catPath[0].split()[-1],return_attribute(f,('category','name')))
                    if cat is None:
                        cat = catPath[-1]
                else:
                    type = catPath[0].lower()
                    cat = (type,return_attribute(f,('category','name')))
                    if cat is None:
                        cat = catPath[-1]
            ElemFlowList.append({
                "categories": cat,
                "code": return_attribute(f,'@id'),
                "CAS number": return_attribute(f,'cas'),
                "database": "biosphere3",
                "name": return_attribute(f,'name'),
                "type": type,
                "unit": self.flow_unit.loc[return_attribute(return_attribute(f,'flowProperties')[0],('flowProperty','@id'))].values[0]
            })
        json_object = json.dumps(ElemFlowList, indent = 4)
        print(pbar)
        return(json_object) 

    def list_methods(self, selected_methods = []):
        try:
            #methods = list(self.get_descriptors(olca.ImpactMethod))
            methods = list(self.get_descriptors(self.olca_module.ImpactMethod))
        except:
            methods = self.lcia_methods
        if selected_methods != all:
            methods = [m for m in methods if return_attribute(m,'name') in selected_methods]
        nb_methods = len(methods)
        pbar = pyprind.ProgBar(nb_methods, title="Extracting "+str(nb_methods)+" LCIA methods from OpenLCA:")
        list_methods = []
        for method in methods:
            pbar.update(item_id = return_attribute(method,'name').ljust(30))
            try:
                impact_categories = method['impactCategories']
            except:
                #impact_categories = return_attribute(self.get(olca.ImpactMethod,uid=method.id),'impactCategories')
                impact_categories = return_attribute(self.get(self.olca_module.ImpactMethod,uid=method.id),'impactCategories')
            if impact_categories is None:
                    continue  
            for c in impact_categories:
                try:
                    cate = [cat for cat in self.lcia_categories if cat['@id'] == return_attribute(c,'@id')][0]
                except:
                    #cate = self.get(olca.ImpactCategory,uid=return_attribute(c,'@id'))
                    cate = self.get(self.olca_module.ImpactCategory,uid=return_attribute(c,'@id'))
                list_cf = []
                impact_factors = return_attribute(cate,'impactFactors')
                if impact_factors is None:
                    continue
                for i in impact_factors:
                    list_cf.append((
                        ('biosphere3',return_attribute(i,('flow','@id'))),
                        return_attribute(i,'value') / self.unit_conv.loc[return_attribute(i,('unit','@id'))].values[0]
                        ))
                list_methods.append({
                    'name': (return_attribute(method,'name'),return_attribute(cate,'name')),
                    'ref_unit': return_attribute(cate,'referenceUnitName') or return_attribute(cate,'refUnit'),
                    'list_cf': list_cf
                    })
        print(pbar)
        return(list_methods)  

    def extract_list_process(self, databases_folders = {}, exclude_S=False, update=False):
        
        dict_process_parameters = {}
        list_missed_providers = []
        databases_ids = {}
        try:
            #processes = list(self.get_descriptors(olca.Process))
            processes = list(self.get_descriptors(self.olca_module.Process))
        except:
            processes = self.processes
        for db in databases_folders.keys():
            list_process_id = []
            for p in processes:
                if root_folder(p) in databases_folders[db]:
                    if exclude_S:
                        if self.change_S_process(p)[0] == False:
                            continue
                    list_process_id.append(return_attribute(p,"@id"))
            databases_ids[db] = list_process_id
        if update:
            #if isinstance(self,olca.Client):
            #    update_process = [self.get(olca.Process, uid=return_attribute(p,'@id')) for p in processes
            if getattr(self,"olca_module",False) and isinstance(self,self.olca_module.Client):
                update_process = [self.get(self.olca_module.Process, uid=return_attribute(p,'@id')) for p in processes
                                    if root_folder(p) in flattenNestedList(list(databases_folders.values()))]
            else:
                update_process = [p for p in self.processes
                                    if root_folder(p) in flattenNestedList(list(databases_folders.values()))]                       
            providers_exch = flattenNestedList([[return_attribute(exc,'defaultProvider') for exc in return_attribute(process,'exchanges') if return_attribute(exc,'defaultProvider')] 
                                for process in update_process])
            providers_exch = [self.params_providers(p) for p in providers_exch]
            self.convert_ids = convert_to_internal_ids(providers_exch)
            databases_ids.update({db: [act['code'] for act in bw.Database(db)] 
                                    for db in bw.databases 
                                    if db != 'biosphere3' and db not in databases_folders.keys()})
        for db in databases_folders.keys():
            pbar = pyprind.ProgBar(len(databases_ids[db]), title="Treatment of openlca processes, "+str(len(databases_ids[db]))+" processes extracted from OpenLCA to "+str(db)+" database")
            list_process = []
            for p_id in databases_ids[db]:
                pbar.update(item_id = len(list_process)+1)
                p_dict = self.build_process_dict(process_id = p_id, database_ids_dic = databases_ids, exclude_S = exclude_S)
                dict_process_parameters.update({(db,p_id): p_dict.pop('list_parameters')})
                list_missed_providers.extend(p_dict.pop('list_missed_providers'))
                list_process.append(p_dict)
            databases_folders[db] = list_process
            print(pbar)
        
        return databases_folders, dict_process_parameters, list_missed_providers    

    def build_process_dict(self,process_id, database_ids_dic, exclude_S):
        try:
            #process = self.get(olca.Process, uid = process_id).to_json()
            process = self.get(self.olca_module.Process, uid = process_id)
            if getattr(process,'to_dict',None):
                process = process.to_dict()
            else:
                process = process.to_json()
        except:
            process = [p for p in self.processes if p["@id"] == process_id][0] 
        classif = [return_attribute(process, ('category','name'))]
        if classif[0] is None:
            classif = return_attribute(process,"category").split("/")
        else:
            classif.insert(0,return_attribute(process, ('category','categoryPath')))
        exch = return_attribute(process,'exchanges')
        ref_exch = [exc for exc in exch if (return_attribute(exc,'quantitativeReference') or return_attribute(exc,'isQuantitativeReference'))][0]
        if return_attribute(ref_exch,('flow','flowType')) == 'WASTE_FLOW':
            cf_waste = -1
        else:
            cf_waste = 1
        p_arg = {'comment': return_attribute(process, 'description') or '',
                'classification': classif,
                'activity': return_attribute(process,'@id'),
                'name': return_attribute(process,'name'),
                'parameters': return_attribute(process,'parameters'),
                'authors': return_attribute(process,('processDocumentation','dataGenerator','name')),
                'type': 'process',
                'code': return_attribute(process,'@id'),
                'reference product': return_attribute(ref_exch,('flow','name')),
                'flow': return_attribute(ref_exch,('flow','@id')),
                'unit': normalize_units(return_attribute(ref_exch,('unit','name'))),
                'unit_id': return_attribute(ref_exch,('unit','@id')),
                'production amount': return_attribute(ref_exch,'amount') * cf_waste,
                'database' : [k for k, v in database_ids_dic.items() if process_id in v][0],
                'list_parameters': [],
                'list_missed_providers': []
                }  
        if return_attribute(process,('location','@id')) is not None:
            p_arg.update({'location': self.location_table.loc[return_attribute(process,('location','@id'))].values[0]})     
        list_exc = []
        for exc in exch:
            exc_dict =self.build_exchange_dict(exchange = exc, process = p_arg, database_ids_dic = database_ids_dic, exclude_S = exclude_S) 
            p_arg['list_missed_providers'].extend(exc_dict.pop('missed_provider'))
            list_exc.append(exc_dict)
        p_arg.update({'exchanges': list_exc})
        if len([exc for exc in p_arg['exchanges'] if is_product(exc)]) > 1:
            p_arg['type'] = 'multioutput'
            p_arg['default allocation'] = return_attribute(process,'defaultAllocationMethod')
            p_arg['allocation factors'] = convert_alloc_factor(return_attribute(process,'allocationFactors'), p_arg)
        if return_attribute(process,'parameters') is not None:
            #if not isinstance(self,olca.Client):
            if not getattr(self,"olca_module",False):
                self.parameters.extend(return_attribute(process,'parameters'))
            p_arg['list_parameters'] = [p['@id'] for p in return_attribute(process,'parameters')]
        return(p_arg)

    def build_exchange_dict(self, exchange, process, database_ids_dic, exclude_S):
        exc_arg = {
            'flow': return_attribute(exchange,('flow','@id')),
            'name': return_attribute(exchange,('flow','name')),
            'unit': normalize_units(return_attribute(exchange,('unit','name'))),
            'unit_id': return_attribute(exchange,('unit','@id')),
            'comment': return_attribute(exchange,'descrition'),
            'internalId': return_attribute(exchange,'internalId'),
            'missed_provider': []
        }
        cf_IO = 1
        if return_attribute(exchange,('flow','flowType')) == 'ELEMENTARY_FLOW':
            if (return_attribute(exchange,'input') == False or return_attribute(exchange,'isInput') == False) and ((return_attribute(exchange,('flow','categoryPath')) or return_attribute(exchange,('flow','category')).split("/"))[1] == 'Resource'):
                cf_IO = -1
            if (return_attribute(exchange,'input') == True or return_attribute(exchange,'isInput') == True) and ((return_attribute(exchange,('flow','categoryPath')) or return_attribute(exchange,('flow','category')).split("/"))[1] != 'Resource'):
                cf_IO = -1
            exc_arg.update({'type': 'biosphere',
                            'input': ('biosphere3',return_attribute(exchange,('flow','@id')))
                            })
        else:
            if return_attribute(exchange,('flow','flowType')) == 'WASTE_FLOW':
                cf_IO = -1
            if return_attribute(exchange,'avoidedProduct') or return_attribute(exchange,'isAvoidedProduct'):
                cf_IO = -cf_IO
            if is_product(exchange):
                exc_arg.update({'type': 'production',
                            'input': (process['database'],process['activity'])
                            })
            else:
                if  return_attribute(exchange,'defaultProvider') is None:
                    exc_arg['missed_provider'] = [{'activity':(process['database'],process['activity']),
                                                'num exchange': return_attribute(exchange,'internalId'),
                                                'flow': return_attribute(exchange,('flow','@id'))}]
                    exc_arg.update({'type': 'technosphere',
                            'input': (process['database'],process['activity'])
                            })
                else:
                    if (return_attribute(exchange,('defaultProvider','@id')) in self.convert_ids.keys()
                        and return_attribute(exchange,('defaultProvider','@id')) != self.convert_ids[return_attribute(exchange,('defaultProvider','@id'))]):
                        id_provider = self.convert_ids[return_attribute(exchange,('defaultProvider','@id'))][1]
                    elif exclude_S:
                        test_S = self.change_S_process(return_attribute(exchange,'defaultProvider'))
                        if test_S[0] == False:
                            id_provider = test_S[1]
                        else:
                            id_provider = return_attribute(exchange,('defaultProvider','@id'))
                    else:
                        id_provider = return_attribute(exchange,('defaultProvider','@id'))
                    try:
                        exc_arg.update({'type': 'technosphere',
                                    'activity': id_provider,
                                    'input': ([k for k, v in database_ids_dic.items() if id_provider in v][0],id_provider)
                                    })
                    except:
                        print({'activity': process['name'],
                                'exchanges': (exc_arg['name'],exc_arg['flow']),
                                'idprovider': id_provider})
        if return_attribute(exchange,'amountFormula') is not None:
            if cf_IO == -1:
                formule = str("-(")+return_attribute(exchange,'amountFormula')+str(")")
            else:
                formule = str("")+return_attribute(exchange,'amountFormula')
            exc_arg.update({'formula': formule})
        exc_arg.update({'amount': return_attribute(exchange,'amount') * cf_IO})
        if return_attribute(exchange,'dqEntry') is not None:
            exc_arg.update({'pedigree': {
                'reliability': return_attribute(exchange,('dqEntry',1)),
                'completeness': return_attribute(exchange,('dqEntry',3)),
                'temporal correlation': return_attribute(exchange,('dqEntry',5)),
                'geographical correlation': return_attribute(exchange,('dqEntry',7)),
                'further technological correlation': return_attribute(exchange,('dqEntry',9))}
            })
        if return_attribute(exchange,'uncertainty') is not None:
            uncertainties = uncertainty_convert(return_attribute(exchange,'uncertainty'),negative=(cf_IO==-1))
            if uncertainties is not None:
                exc_arg.update(uncertainties)
        return(exc_arg)