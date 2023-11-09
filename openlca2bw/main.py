# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 22:48:13 2021

@author: cyrille.francois
"""
###import of olca module, as a combine of olca_schema and olca_ipc
from importlib.metadata import version
if version("olca_ipc") == "0.0.12":
    import olca
else:
    import olca_schema as olca
    import olca_ipc as ipc
    olca.Client = ipc.Client


import brightway2 as bw
from . import *
from .IPC_Extractor import IPC_Extractor
from .Json_Extractor import Json_Extractor
from urllib3.connection import HTTPConnection

def load_openLCA_IPC(port = 8080, project_name="Open_imports",overwrite=False, 
                     nonuser_db_name = 'EcoInvent',check_nonuser_exc=False,
                     user_databases={},excluded_folders=[], exclude_S=False, selected_methods= all,
                     verbose=False,olca_module=olca):
    '''
    Function to load an OpenLCA database, when OpenLCA and IPC are both active.

    Args
    ----------
    port : int
        Number of IPC Server previously open in OpenLCA
    project_name : str
        Name of the brightway project. If it doesn't exist, it gets created and set as current by the end of the routine.
    overwrite : Bool
        Overwrites existing database with the same name. 
    nonuser_db_name : str
        Name for the BW database to import all Processes of the project that are not include in `user_databases` folders.
    check_nonuser_exc : Bool
        Launch the verification of all exchanges in the non-users databases, if exchanges units are coherent.
    user_databases : dict
        Dictionary consisting of new brightway database name as keys, and the list of OpenLCA Category folders (root folders) as values (could be just a name). For example: {'FirstDatabase': ['Folder1','Folder2'],'SecondDatabase': 'Folder3'}. 
    excluded_folders : list 
        List of OpenLCA Category folders (root folders) names as strings to exclude from import.
    exclude_s : Bool
        When using both 'Unit process' and 'System process', when excluding System processes (exclude_S = True), the import will prioritize Unit process when it exist. Save large amount of time.
    selected_methods : 
        List of OpenLCA LCIA methods names as strings that will be imported in brightway. default value is 'all'.
    verbose : Bool
        Print all exchanges deleted due to provider issue

    Returns
    -------
    No returns, just activates the brightway project specified with the imported database loaded into it.
    '''

    #Create connection with the OpenLCA IPC protocol
    try:
         HTTPConnection(host='localhost', port=port).connect()
    except:
        print('Error with IPC connection, verify IPC connection and port in OpenLCA')
        return

    print('Checking of OpenLCA version\n')
    olca_module = check_OpenLCA_version(olca_module)
    
    print('Creation of background tables (units, locations)\n')
    client = IPC_Extractor(port)
    client.flow_unit = client.flow_properties_unit()
    client.unit_conv = client.unit_convert_factor()
    client.location_table = client.location_convert()
    client.change_param = change_param_names([p.name for p in client.get_all(olca.Parameter) if return_attribute(p,'parameterScope') == 'GLOBAL_SCOPE'])
    
    #Create or replace the brightway project
    if project_name in bw.projects and overwrite == False:
        print("Project {} already exists and overwrite is False".format(project_name))
        return
    if project_name in list(bw.databases) and overwrite == True:
        bw.projects.delete_project(project_name, delete_dir=True)
    bw.projects.set_current(project_name)
    #projects don't seems to be completly deleted with delete_project
    for db in list(bw.databases):
        del bw.databases[db]
    for m in list(bw.methods):
        del bw.methods[m]
    print("Project "+project_name+ " created on brightway environment\n")
    
    #Biosphere creation
    if "biosphere3" in bw.databases:
        print("Biosphere database already present!!! No setup is needed")
        return
    print("\nCreating OpenLCA biosphere\n")
    json_biosphere = client.json_elementary_flow()
    create_OpenLCA_biosphere3(json_biosphere)
    
    #LCIA methods creation
    print("\nCreating OpenLCA LCIA methods\n")
    create_OpenLCA_LCIAmethods(client.list_methods(selected_methods))
    
    #Separation of processes based on the input "user_databases"
    #process_folders = [return_attribute(c,'name') for c in client.get_all(olca.Category) 
    #                    if return_attribute(c,'modelType') == 'PROCESS' 
    #                        and return_attribute(c,'category') is None
    #                        and return_attribute(c,'name') not in excluded_folders]
    process_folders = list(set([root_folder(p) for p in client.get_descriptors(olca.Process)
                            if root_folder(p) not in excluded_folders]))
    databases_folders = {**{nonuser_db_name: [c for c in process_folders if c not in flattenNestedList(list(user_databases.values()))]},
                    **user_databases}
    
    #Extraction data
    print("\nImporting processes from OpenLCA \n")
    dict_processes, list_parameters, dict_missed_providers = client.extract_list_process(databases_folders = databases_folders,exclude_S = exclude_S, update=False)
    #Writing data
    for db, list_process in dict_processes.items():
        write_db = bw.Database(db)
        write_db.write(dict(zip([(db,p['code']) for p in list_process],list_process)))

    #Provider finder, retrieve provider in case of unspecified and single provider. Activity deleted if several providers
    single_provider_retriver(dict_missed_providers, verbose=verbose)
    
    #Check the uniformity of unit for exchange
    #Brightway don't handle the unit conversion
    #for example error appears when electricity production is express per kWh but it's use as input with other units (ex: MJ)
    if check_nonuser_exc:
        check_exchanges_units(client.unit_conv,nonuser_db_name)
    check_exchanges_units(client.unit_conv,list(user_databases.keys()))
    
    #Parameters express in activity have to be imported separetly in brightway
    print('\nImporting parameters from OpenLCA\n')
    import_parameters(client.get_all(olca.Parameter),list_parameters, client.change_param)

def update_openLCA_IPC(port = 8080, project_name="Open_imports",update_biosphere=False,update_methods=[],
                     update_databases={}, exclude_S=False,olca_module=olca):
    '''
    Create connection with the OpenLCA IPC protocol

    Args
    ----------
    port : int
        Number of IPC Server previously open in OpenLCA
    project_name : str
        Name of the brightway project. Projects need to be created before. It will be set as current by the end of the routine.
    update_biosphere : Bool
        Update the biosphere boolean by importing all OpenLCA Elementary Flows.
    update_methods : Bool
        List of OpenLCA LCIA methods names as strings that will be imported in brightway. 
    update_databases : dict
        Dictionary consisting of brightway database name as keys, and the list of OpenLCA Category folders (root folders) as values (could be just a name). If brightway database already exist it will be replaced, else it will be created.
    excluded_S : Bool
        When using both 'Unit process' and 'System process', when excluding System processes (exclude_S = True), the import will prioritize Unit process when it exist. Save large amount of time.
    verbose : Bool
        Print all exchanges deleted due to provider issue

    Returns
    -------
    No returns, just activates the brightway project specified with the imported database loaded into it.
    '''
    
    #Create connection with the OpenLCA IPC protocol
    try:
         HTTPConnection(host='localhost', port=port).connect()
    except:
        print('Error with IPC connection, verify IPC connection and port in OpenLCA')
        return
   
    print('Checking of OpenLCA version\n')
    olca_module = check_OpenLCA_version(olca_module)
   
    print('Creation of background tables (units, locations)\n')
    client = IPC_Extractor(port,olca_module)
    client.flow_unit = client.flow_properties_unit()
    client.unit_conv = client.unit_convert_factor()
    client.location_table = client.location_convert()
    client.change_param = change_param_names([p.name for p in client.get_all(olca_module.Parameter) if return_attribute(p,'parameterScope') == 'GLOBAL_SCOPE'])
    
    #Select the brightway project
    if project_name not in bw.projects:
        print("Project {} not present in brightway".format(project_name))
        return
    bw.projects.set_current(project_name)
    print("Updating of project "+project_name+"\n")
     
    #Biosphere creation
    if update_biosphere:
        print("Update of biosphere flows")
        json_biosphere = client.json_elementary_flow()
        del bw.databases['biosphere3']
        create_OpenLCA_biosphere3(json_biosphere)
        
    #Methods update
    if len(update_methods)>0:
        print("Update of methods")
        methods = client.list_methods(update_methods)
        for m in methods:
            if m['name'] not in bw.methods:
                register_method(methodName=m['name'],methodUnit=m['ref_unit'],method_data=m['list_cf'])
            elif m['ref_unit'] != bw.methods[m['name']]['unit'] or len(m['list_cf']) != bw.methods[m['name']]['num_cfs']:
                del bw.methods[m['name']]
                register_method(methodName=m['name'],methodUnit=m['ref_unit'],method_data=m['list_cf'])
            else:
                bw_cfs = set( frozenset(item) for item in bw.Method(m['name']).load())
                m_cfs = set( frozenset(item) for item in m['list_cf'])
                if m_cfs != bw_cfs:
                    del bw.methods[m['name']]
                    register_method(methodName=m['name'],methodUnit=m['ref_unit'],method_data=m['list_cf'])
    
    print("\nImporting processes from OpenLCA\n")
    dict_processes, user_parameters, list_missed_providers = client.extract_list_process(databases_folders = update_databases,exclude_S = exclude_S, update=True)
    dict_processes = check_elementary_exchanges(dict_processes)
    for db in list(update_databases.keys()):
        if db in list(bw.databases):
            del bw.databases[db]
        user_db = bw.Database(db)
        user_db.write(dict(zip([(db,l['code']) for l in dict_processes[db]],dict_processes[db])))
    #Provider finder, retrieve provider in case of unspecified and single provider. Activity deleted if several providers
    single_provider_retriver(list_missed_providers, verbose=True)
    #Check the uniformity of unit for exchange
    #Brightway don't handle the unit conversion
    #for example error appears when electricity production is express per kWh but it's use as input with other units (ex: MJ)
    check_exchanges_units(client.unit_conv,list(update_databases.keys()))
    
    #Parameters express in activity have to be imported separetly in brightway
    print('\nImporting parameters from OpenLCA\n')
    import_parameters(client.get_all(olca_module.Parameter),user_parameters, client.change_param)
    

def load_openLCA_Json(path_zip=str, project_name="Open_imports",overwrite=False, 
                     nonuser_db_name = 'EcoInvent',check_nonuser_exc=False,
                     user_databases={},excluded_folders=[], exclude_S=False, selected_methods = all, verbose=False):
    '''
    Function to load an OpenLCA database that has been exported as a Json-LD zip file and uncompressed.

    Args
    ----------
    path_zip : str
        Path to the zip file, or to the folder containing the unziped database, exported from OpenLCA in Json-LD format.
    project_name : str
        Name of the brightway project. If it doesn't exist, it gets created and set as current by the end of the routine.
    overwrite : Bool
        Overwrites existing database with the same name. 
    nonuser_db_name : str
        Name for the BW database to import all Processes of the project that are not include in `user_databases` folders.
    check_nonuser_exc : Bool
        Launch the verification of all exchanges in the non-users databases, if exchanges units are coherent.
    user_databases : dict
        Dictionary consisting of new brightway database name as keys, and the list of OpenLCA Category folders (root folders) as values (could be just a name). For example: {'FirstDatabase': ['Folder1','Folder2'],'SecondDatabase': 'Folder3'}. 
    excluded_folders : list 
        List of OpenLCA Category folders (root folders) names as strings to exclude from import.
    exclude_s : Bool
        When using both 'Unit process' and 'System process', when excluding System processes (exclude_S = True), the import will prioritize Unit process when it exist. Save large amount of time.
    selected_methods : 
        List of OpenLCA LCIA methods names as strings that will be imported in brightway. default value is 'all'.
    verbose : Bool
        Print all exchanges deleted due to provider issue

    Returns
    -------
    No returns, just activates the brightway project specified with the imported database loaded into it.
    '''

    #Create or replace the brightway project
    if project_name in bw.projects and overwrite == False:
        print("Project {} already exists and overwrite is False".format(project_name))
        return
    if project_name in list(bw.databases) and overwrite == True:
        bw.projects.delete_project(project_name, delete_dir=True)
    bw.projects.set_current(project_name)
    #projects don't seems to be completly deleted with delete_project
    for db in list(bw.databases):
        del bw.databases[db]
    for m in list(bw.methods):
        del bw.methods[m]
    print("Project "+project_name+ " created on brightway environment\n")
    
    #Extract and open json Zip
    print('Extraction and  pre-treatment of json zip folder\n')
    json_db = Json_Extractor()
    json_db.extract_zip_openlca(zip_path=path_zip)
    json_db.flow_unit = json_db.flow_properties_unit()
    json_db.location_table = json_db.location_convert()
    json_db.unit_conv = json_db.unit_convert_factor()
    json_db.change_param = change_param_names([p["name"] for p in json_db.parameters if return_attribute(p,'parameterScope') == 'GLOBAL_SCOPE'])
    
    #Biosphere creation
    if "biosphere3" in bw.databases:
        print("Biosphere database already present!!! No setup is needed")
        return
    print("\nCreating OpenLCA biosphere\n")
    json_biosphere = json_db.json_elementary_flow()
    create_OpenLCA_biosphere3(json_biosphere)
    
    #LCIA methods creation
    print("\nCreating OpenLCA LCIA methods\n")
    create_OpenLCA_LCIAmethods(json_db.list_methods(selected_methods))
    
    process_folders = [c['name'] for c in json_db.categories 
                        if return_attribute(c,'modelType') == 'PROCESS' 
                            and return_attribute(c,'category') is None
                            and c['name'] not in excluded_folders]
    databases_folders = {**{nonuser_db_name: [c for c in process_folders if c not in flattenNestedList(list(user_databases.values()))]},
                    **user_databases}
    #Extraction data
    print("\nImporting processes from OpenLCA \n")
    dict_processes, list_parameters, dict_missed_providers = json_db.extract_list_process(databases_folders = databases_folders,exclude_S = exclude_S, update=False)
    #Writing data
    for db, list_process in dict_processes.items():
        write_db = bw.Database(db)
        write_db.write(dict(zip([(db,p['code']) for p in list_process],list_process)))

    #Provider finder, retrieve provider in case of unspecified and single provider. Activity deleted if several providers
    single_provider_retriver(dict_missed_providers, verbose=verbose)
    #The complete checking of units for large database like EcoInvent is long
    #To run the checking specify the input "check_nonuser_exc" with True
    #EcoInvent don't have issue with units (as far as I know)
    if check_nonuser_exc:
        check_exchanges_units(json_db.unit_conv,nonuser_db_name)
    check_exchanges_units(json_db.unit_conv,list(user_databases.keys()))
        
    #Parameters express in activity have to be imported separetly in brightway
    print('\nImporting parameters from OpenLCA\n')
    if len(json_db.parameters)>0:
        import_parameters(json_db.parameters,list_parameters,json_db.change_param)

    
    
def update_openLCA_Json(path_zip=str, project_name="Open_imports",update_biosphere=False,update_methods=[],
                     update_databases={}, exclude_S=False, verbose=False):
    '''

    Args
    ----------
    path_zip : int
        Path to the folder containing the unziped database exported from OpenLCA in Json-LD format.
    project_name : str
        Name of the brightway project. Projects need to be created before. It will be set as current by the end of the routine.
    update_biosphere : Bool
        Update the biosphere boolean by importing all OpenLCA Elementary Flows.
    update_methods : Bool
        List of OpenLCA LCIA methods names as strings that will be imported in brightway. 
    update_databases : dict
        Dictionary consisting of brightway database name as keys, and the list of OpenLCA Category folders (root folders) as values (could be just a name). If brightway database already exist it will be replaced, else it will be created.
    excluded_S : Bool
        When using both 'Unit process' and 'System process', when excluding System processes (exclude_S = True), the import will prioritize Unit process when it exist. Save large amount of time.
    verbose : Bool
        Print all exchanges deleted due to provider issue

    Returns
    -------
    No returns, just activates the brightway project specified with the imported database loaded into it.
    '''
    
    #Select the brightway project
    if project_name not in bw.projects:
        print("Project {} not present in brightway".format(project_name))
        return
    bw.projects.set_current(project_name)
    print("Updating of project "+project_name+"\n")
    #Extract and open json Zip
    print('Extraction and  pre-treatment of json zip folder\n')
    json_db = Json_Extractor()
    zip_folders = ['categories','unit_groups','flow_properties','locations','flows','lcia_categories','lcia_methods','processes','parameters']
    if not update_biosphere:
        zip_folders.remove('flows')
    if not update_methods:
        zip_folders = [f for f in zip_folders if f not in ['lcia_categories','lcia_methods']]
    json_db.extract_zip_openlca(zip_path=path_zip,folders=zip_folders)
    if len(json_db.flow_properties) > 0:
        json_db.flow_unit = json_db.flow_properties_unit()
    if len(json_db.unit_groups) > 0:
        json_db.unit_conv = json_db.unit_convert_factor()
    if len(json_db.locations) > 0:
         json_db.location_table = json_db.location_convert()
    if len(json_db.parameters) > 0:
        json_db.change_param = change_param_names([p["name"] for p in json_db.parameters if return_attribute(p,'parameterScope') == 'GLOBAL_SCOPE'])
      
    #Biosphere creation
    if update_biosphere:
        print("Update of biosphere flows")
        json_biosphere = json_db.json_elementary_flow()
        del bw.databases['biosphere3']
        create_OpenLCA_biosphere3(json_biosphere)
        
    #Methods update
    if update_methods != []:
        print("Update of methods")
        methods = json_db.list_methods(update_methods)
        for m in methods:
            if m['name'] not in bw.methods:
                register_method(methodName=m['name'],methodUnit=m['ref_unit'],method_data=m['list_cf'])
            elif m['ref_unit'] != bw.methods[m['name']]['unit'] or len(m['list_cf']) != bw.methods[m['name']]['num_cfs']:
                del bw.methods[m['name']]
                register_method(methodName=m['name'],methodUnit=m['ref_unit'],method_data=m['list_cf'])
            else:
                bw_cfs = set( frozenset(item) for item in bw.Method(m['name']).load())
                m_cfs = set( frozenset(item) for item in m['list_cf'])
                if m_cfs != bw_cfs:
                    del bw.methods[m['name']]
                    register_method(methodName=m['name'],methodUnit=m['ref_unit'],method_data=m['list_cf'])

    print("\nImporting processes from OpenLCA\n")
    #dict_processes, user_parameters, list_missed_providers = json_db.extract_list_process(databases_names = update_databases, dict_list_id=db_list_id,exclude_S = exclude_S,update=True)
    dict_processes, user_parameters, list_missed_providers = json_db.extract_list_process(databases_folders = update_databases,exclude_S = exclude_S, update=True)
    dict_processes = check_elementary_exchanges(dict_processes)
    for db in list(update_databases.keys()):
        if db in list(bw.databases):
            del bw.databases[db]
        user_db = bw.Database(db)
        user_db.write(dict(zip([(db,l['code']) for l in dict_processes[db]],dict_processes[db])))
    #Provider finder, retrieve provider in case of unspecified and single provider. Activity deleted if several providers
    single_provider_retriver(list_missed_providers, verbose=verbose)
    #Check the uniformity of unit for exchange
    #Brightway don't handle the unit conversion
    #for example error appears when electricity production is express per kWh but it's use as input with other units (ex: MJ)
    check_exchanges_units(json_db.unit_conv,list(update_databases.keys()))
    
    #Parameters express in activity have to be imported separetly in brightway
    print('\nImporting parameters from OpenLCA\n')
    if len(json_db.parameters)>0:
        import_parameters(json_db.parameters,user_parameters,json_db.change_param)
  
