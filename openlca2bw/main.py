# -*- coding: utf-8 -*-
"""
Created on Fri Jul  2 22:48:13 2021

@author: cyrille.francois
"""
import olca
import brightway2 as bw
from .IPC_protocol import *
from . import *
from urllib3.connection import HTTPConnection

def load_openLCA_IPC(port = 8080, project_name="Open_imports",overwrite=False, 
                     nonuser_db_name = 'EcoInvent',check_ecoinvent_exc=False,
                     user_databases={}):
    #Create connection with the OpenLCA IPC protocol
    try:
         HTTPConnection(host='localhost', port=port).connect()
    except ConnectionError:
        print('Error with IPC connection, verify IPC connection and port in OpenLCA')
        return
    client = ClientData(port)
    
    #Create or replace the brightway project
    if project_name in list(bw.databases) and overwrite == False:
        print("Project {} already exists and overwrite is False".format(project_name))
        return
    if project_name in list(bw.databases) and overwrite == True:
        bw.projects.delete_project(project_name)
    bw.projects.set_current(project_name)
    
    #Call functions that are related to IPC connection
    client.flow_properties_unit = flow_properties_unit.__get__(client)           
    client.json_elementary_flow = json_elementary_flow.__get__(client)
    client.unit_convert_factor = unit_convert_factor.__get__(client)
    client.location_convert = location_convert.__get__(client)
    client.list_methods = list_methods.__get__(client)
    client.create_OpenLCA_LCIAmethods = create_OpenLCA_LCIAmethods.__get__(client) 
    client.list_nonuser_process = list_nonuser_process.__get__(client)
    client.extract_list_process = extract_list_process.__get__(client)
    client.import_parameters = import_parameters.__get__(client)
    client.check_exchanges_units = check_exchanges_units.__get__(client)
    
    client.flow_unit = client.flow_properties_unit()
    client.unit_conv = client.unit_convert_factor()
    client.location_table = client.location_convert()
    
    #Biosphere creation
    if "biosphere3" in bw.databases:
        print("Biosphere database already present!!! No setup is needed")
        return
    print("\nCreating OpenLCA biosphere\n")
    json_biosphere = client.json_elementary_flow()
    create_OpenLCA_biosphere3(json_biosphere)
    
    #LCIA methods creation
    print("\nCreating OpenLCA LCIA methods\n")
    client.create_OpenLCA_LCIAmethods()
    
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
    nonuser_folders = [f for f in process_folders if f not in user_folders]
    
    #Extraction and writing of non-user data (ex: EcoInvent processes)    
    print("\nImporting Non-User processes from OpenLCA (ex: EcoInvent)\n")
    NonUser_processes, list_parameters = client.list_nonuser_process(database_name = nonuser_db_name,nonuser_folders=nonuser_folders)
    nonuser_ids = [i['code'] for i in NonUser_processes]
    db = bw.Database(nonuser_db_name)
    db.write(dict(zip([(nonuser_db_name,l['code']) for l in NonUser_processes],NonUser_processes)))
    #The complete checking of units for large database like EcoInvent is long
    #To run the checking specify the input "check_ecoinvent_exc" with True
    #EcoInvent don't have issue with units (as far as I know)
    if check_ecoinvent_exc:
        client.check_exchanges_units(nonuser_db_name)
        
    #Extraction and writing of user data      
    print("\nImporting others processes from OpenLCA\n")
    dict_processes, user_parameters = client.extract_list_process(databases_names = user_databases, list_nonuser_id = nonuser_ids, db_nonuser_name = nonuser_db_name)
    for db in list(user_databases.keys()):
        user_db = bw.Database(db)
        user_db.write(dict(zip([(db,l['code']) for l in dict_processes[db]],dict_processes[db])))
    #Check the uniformity of unit for exchange
    #Brightway don't handle the unit conversion
    #for example error appears when electricity production is express per kWh but it's use as input with other units (ex: MJ)
    client.check_exchanges_units(list(user_databases.keys()))
    
    #Parameters express in activity have to be imported separetly in brightway
    print('\nImporting parameters from OpenLCA\n')
    list_parameters.extend(user_parameters)
    client.import_parameters(list_parameters)
