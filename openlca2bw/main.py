# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 22:48:13 2021

@author: cyrille.francois
"""
import olca
import brightway2 as bw
from .IPC_protocol import *
from . import *
from .json_protocol import Json_database, Json_All
from urllib3.connection import HTTPConnection

def load_openLCA_IPC(port = 8080, project_name="Open_imports",overwrite=False, 
                     nonuser_db_name = 'EcoInvent',check_nonuser_exc=False,
                     user_databases={},excluded_folders=[], exclude_S=False):
    #Create connection with the OpenLCA IPC protocol
    try:
         HTTPConnection(host='localhost', port=port).connect()
    except:
        print('Error with IPC connection, verify IPC connection and port in OpenLCA')
        return
    print('Creation of background tables (units, locations)\n')
    client = ClientAll(port)
    #Create or replace the brightway project
    if project_name in bw.projects and overwrite == False:
        print("Project {} already exists and overwrite is False".format(project_name))
        return
    if project_name in list(bw.databases) and overwrite == True:
        bw.projects.delete_project(project_name)
    bw.projects.set_current(project_name)
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
    create_OpenLCA_LCIAmethods(client.list_methods())
    
    #Separation of processes based on the input "user_databases"
    category = client.get_all(olca.Category)
    process_folders = []
    while True:
        try:
            c = next(category)
            if c.model_type.name == 'PROCESS' and c.category is None:
                process_folders.append(c.name)
        except StopIteration:
            break
        except ValueError:
            pass
    user_folders = flattenNestedList([c for c in user_databases.values()])
    nonuser_folders = [f for f in process_folders if f not in user_folders and f not in excluded_folders]
    
    #Extraction and writing of non-user data (ex: EcoInvent processes)    
    print("\nImporting Non-User processes from OpenLCA (ex: EcoInvent)\n")
    NonUser_processes, list_parameters, list_missed_providers = client.list_nonuser_process(database_name = nonuser_db_name,nonuser_folders=nonuser_folders, exclude_S = exclude_S)
    nonuser_ids = [i['code'] for i in NonUser_processes]
    db = bw.Database(nonuser_db_name)
    db.write(dict(zip([(nonuser_db_name,l['code']) for l in NonUser_processes],NonUser_processes)))
    #Provider finder, retrieve provider in case of unspecified and single provider. Activity deleted if several providers
    single_provider_retriver(list_missed_providers)
    #The complete checking of units for large database like EcoInvent is long
    #To run the checking specify the input "check_nonuser_exc" with True
    #EcoInvent don't have issue with units (as far as I know)
    if check_nonuser_exc:
        check_exchanges_units(client.unit_conv,nonuser_db_name)
        
    #Extraction and writing of user data      
    print("\nImporting others processes from OpenLCA\n")
    dict_processes, user_parameters, list_missed_providers = client.extract_list_process(databases_names = user_databases, dict_list_id={nonuser_db_name: nonuser_ids}, exclude_S = exclude_S)
    for db in list(user_databases.keys()):
        user_db = bw.Database(db)
        user_db.write(dict(zip([(db,l['code']) for l in dict_processes[db]],dict_processes[db])))
    #Provider finder, retrieve provider in case of unspecified and single provider. Activity deleted if several providers
    single_provider_retriver(list_missed_providers)
    #Check the uniformity of unit for exchange
    #Brightway don't handle the unit conversion
    #for example error appears when electricity production is express per kWh but it's use as input with other units (ex: MJ)
    check_exchanges_units(client.unit_conv,list(user_databases.keys()))
    
    #Parameters express in activity have to be imported separetly in brightway
    print('\nImporting parameters from OpenLCA\n')
    list_parameters.extend(user_parameters)
    import_parameters_ipc(client.get_all(olca.Parameter),list_parameters)

def update_openLCA_IPC(port = 8080, project_name="Open_imports",update_biosphere=False,update_methods=False,
                     update_databases={}, exclude_S=False):
    #Create connection with the OpenLCA IPC protocol
    try:
         HTTPConnection(host='localhost', port=port).connect()
    except:
        print('Error with IPC connection, verify IPC connection and port in OpenLCA')
        return
    # client = ClientData(port)
    print('Creation of background tables (units, locations)\n')
    client = ClientAll(port)
    
    #Select the brightway project
    if project_name not in bw.projects:
        print("Project {} not present in brightway".format(project_name))
        return
    bw.projects.set_current(project_name)
    print("Updating of project "+project_name+"\n")
    
    # #Call functions that are related to IPC connection
    # client.flow_properties_unit = flow_properties_unit.__get__(client)           
    # client.json_elementary_flow = json_elementary_flow.__get__(client)
    # client.unit_convert_factor = unit_convert_factor.__get__(client)
    # client.location_convert = location_convert.__get__(client)
    # client.list_methods = list_methods.__get__(client)
    # client.create_OpenLCA_LCIAmethods = create_OpenLCA_LCIAmethods.__get__(client) 
    # client.list_nonuser_process = list_nonuser_process.__get__(client)
    # client.extract_list_process = extract_list_process.__get__(client)
    # client.import_parameters = import_parameters.__get__(client)
    # client.check_exchanges_units = check_exchanges_units.__get__(client)
    # client.U_equivalent = U_equivalent.__get__(client)
    # client.keep_U_process = keep_U_process.__get__(client)
    # print('Creation of background tables (units, locations)\n')
    # client.flow_unit = client.flow_properties_unit()
    # client.unit_conv = client.unit_convert_factor()
    # client.location_table = client.location_convert()    
    
    
    #Biosphere creation
    if update_biosphere:
        print("Update of biosphere flows")
        json_biosphere = client.json_elementary_flow()
        del bw.databases['biosphere3']
        create_OpenLCA_biosphere3(json_biosphere)
        
    #Methods update
    if update_methods:
        print("Update of methods")
        methods = client.list_methods()
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
    
    db_list_id = []
    for db in bw.databases:
        if db == "biosphere3":
            continue
        else:
            db_list_id.append([act['code'] for act in bw.Database(db)])
    db_list_id = dict(zip([db for db in bw.databases if db != 'biosphere3'],db_list_id))
    [db_list_id.pop(key,None) for key in update_databases.keys()]
    print("\nImporting processes from OpenLCA\n")
    dict_processes, user_parameters, list_missed_providers = client.extract_list_process(databases_names = update_databases, dict_list_id=db_list_id,exclude_S = exclude_S)
    for db in list(update_databases.keys()):
        if db in list(bw.databases):
            del bw.databases[db]
        user_db = bw.Database(db)
        user_db.write(dict(zip([(db,l['code']) for l in dict_processes[db]],dict_processes[db])))
    #Provider finder, retrieve provider in case of unspecified and single provider. Activity deleted if several providers
    single_provider_retriver(list_missed_providers)
    #Check the uniformity of unit for exchange
    #Brightway don't handle the unit conversion
    #for example error appears when electricity production is express per kWh but it's use as input with other units (ex: MJ)
    check_exchanges_units(client.unit_conv,list(update_databases.keys()))
    
    #Parameters express in activity have to be imported separetly in brightway
    print('\nImporting parameters from OpenLCA\n')
    import_parameters_ipc(client.get_all(olca.Parameter),user_parameters)
    

def load_openLCA_Json(path_zip=str, project_name="Open_imports",overwrite=False, 
                     nonuser_db_name = 'EcoInvent',check_nonuser_exc=False,
                     user_databases={},excluded_folders=[], exclude_S=False):
    #Create or replace the brightway project
    if project_name in bw.projects and overwrite == False:
        print("Project {} already exists and overwrite is False".format(project_name))
        return
    if project_name in list(bw.databases) and overwrite == True:
        bw.projects.delete_project(project_name)
    bw.projects.set_current(project_name)
    print("Project "+project_name+ " created on brightway environment\n")
    
    #Extract and open json Zip
    print('Extraction and  pre-treatment of json zip folder\n')
    json_db = Json_All()
    json_db.extract_zip_openlca(zip_path=path_zip)
    json_db.flow_properties_unit()
    json_db.location_convert()
    json_db.unit_convert_factor()
    
    #Biosphere creation
    if "biosphere3" in bw.databases:
        print("Biosphere database already present!!! No setup is needed")
        return
    print("\nCreating OpenLCA biosphere\n")
    json_biosphere = json_db.json_elementary_flow()
    create_OpenLCA_biosphere3(json_biosphere)
    
    #LCIA methods creation
    print("\nCreating OpenLCA LCIA methods\n")
    create_OpenLCA_LCIAmethods(json_db.list_methods())
    
    #Separation of processes based on the input "user_databases"
    category = json_db.categories
    process_folders = []
    for c in category:
       if return_attribute(c,'modelType') == 'PROCESS' and return_attribute(c,'category') is None:
           process_folders.append(c['name'])
    user_folders = flattenNestedList([c for c in user_databases.values()])
    nonuser_folders = [f for f in process_folders if f not in user_folders and f not in excluded_folders]
    
    #Extraction and writing of non-user data (ex: EcoInvent processes)    
    print("\nImporting Non-User processes from OpenLCA (ex: EcoInvent)\n")
    NonUser_processes, list_parameters, list_missed_providers = json_db.list_nonuser_process(database_name = nonuser_db_name,nonuser_folders=nonuser_folders, exclude_S = exclude_S)
    nonuser_ids = [i['code'] for i in NonUser_processes]
    db = bw.Database(nonuser_db_name)
    db.write(dict(zip([(nonuser_db_name,l['code']) for l in NonUser_processes],NonUser_processes)))
    #Provider finder, retrieve provider in case of unspecified and single provider. Activity deleted if several providers
    single_provider_retriver(list_missed_providers)
    #The complete checking of units for large database like EcoInvent is long
    #To run the checking specify the input "check_nonuser_exc" with True
    #EcoInvent don't have issue with units (as far as I know)
    if check_nonuser_exc:
        check_exchanges_units(json_db.unit_conv,nonuser_db_name)
        
    #Extraction and writing of user data      
    print("\nImporting others processes from OpenLCA\n")
    dict_processes, user_parameters, list_missed_providers = json_db.extract_list_process(databases_names = user_databases, dict_list_id={nonuser_db_name: nonuser_ids}, exclude_S = exclude_S)
    for db in list(user_databases.keys()):
        user_db = bw.Database(db)
        user_db.write(dict(zip([(db,l['code']) for l in dict_processes[db]],dict_processes[db])))
    #Provider finder, retrieve provider in case of unspecified and single provider. Activity deleted if several providers
    single_provider_retriver(list_missed_providers)
    #Check the uniformity of unit for exchange
    #Brightway don't handle the unit conversion
    #for example error appears when electricity production is express per kWh but it's use as input with other units (ex: MJ)
    check_exchanges_units(json_db.unit_conv,list(user_databases.keys()))
    
    #Parameters express in activity have to be imported separetly in brightway
    print('\nImporting parameters from OpenLCA\n')
    list_parameters.extend(user_parameters)
    import_parameters_json(json_db.parameters,list_parameters)
    
    
def update_openLCA_Json(path_zip=str, project_name="Open_imports",update_biosphere=False,update_methods=False,
                     update_databases={}, exclude_S=False):
    #Select the brightway project
    if project_name not in bw.projects:
        print("Project {} not present in brightway".format(project_name))
        return
    bw.projects.set_current(project_name)
    print("Updating of project "+project_name+"\n")
    
    #Extract and open json Zip
    print('Extraction and  pre-treatment of json zip folder\n')
    json_db = Json_All()
    zip_folders = ['categories','unit_groups','flow_properties','locations','flows','lcia_categories','lcia_methods','processes','parameters']
    if update_biosphere:
        zip_folders.remove('flows')
    if update_methods:
        zip_folders = [f for f in zip_folder if f not in ['lcia_categories','lcia_methods']]
    json_db.extract_zip_openlca(zip_path=path_zip,folders=zip_folders)
    if len(json_db.flow_properties) > 0:
        json_db.flow_properties_unit()
    if len(json_db.unit_groups) > 0:
        json_db.unit_convert_factor()
    if len(json_db.locations) > 0:
         json_db.location_convert()
      
    #Biosphere creation
    if update_biosphere:
        print("Update of biosphere flows")
        json_biosphere = json_db.json_elementary_flow()
        del bw.databases['biosphere3']
        create_OpenLCA_biosphere3(json_biosphere)
        
    #Methods update
    if update_methods:
        print("Update of methods")
        methods = json_db.list_methods()
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
    
    db_list_id = []
    for db in bw.databases:
        if db == "biosphere3":
            continue
        else:
            db_list_id.append([act['code'] for act in bw.Database(db)])
    db_list_id = dict(zip([db for db in bw.databases if db != 'biosphere3'],db_list_id))
    [db_list_id.pop(key,None) for key in update_databases.keys()]
    print("\nImporting processes from OpenLCA\n")
    dict_processes, user_parameters, list_missed_providers = json_db.extract_list_process(databases_names = update_databases, dict_list_id=db_list_id,exclude_S = exclude_S)
    for db in list(update_databases.keys()):
        if db in list(bw.databases):
            del bw.databases[db]
        user_db = bw.Database(db)
        user_db.write(dict(zip([(db,l['code']) for l in dict_processes[db]],dict_processes[db])))
    #Provider finder, retrieve provider in case of unspecified and single provider. Activity deleted if several providers
    single_provider_retriver(list_missed_providers)
    #Check the uniformity of unit for exchange
    #Brightway don't handle the unit conversion
    #for example error appears when electricity production is express per kWh but it's use as input with other units (ex: MJ)
    check_exchanges_units(json_db.unit_conv,list(update_databases.keys()))
    
    #Parameters express in activity have to be imported separetly in brightway
    print('\nImporting parameters from OpenLCA\n')
    import_parameters_json(json_db.parameters,user_parameters)
  