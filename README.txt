These a Python package to import OpenLCA database to the brightway python environment

To install:
pip install <path to setup folder>
for exemple C:/user/userprofile/document/openlca2bw

the main function to import olca database to brightway is load_openLCA_IPC()

To import you need to lauch OpenLCA, to open your database and activate the IPC protocol from the OpenLCA software (Tool/Developer tools/IPC server)
The default port value is 8080

The function has default values and can be run without entries, nontheless all processes will be stored in one unique bw2 database ('EcoInvent').
To split OpenLCA into several database you need to specify the databases you want to create and the OpenLCA folders related.
The format of "user_databases" entry is a dictionnary :

user_databases = {'FirstDatabase': ['Folder1','Folder2',...],'SecondDatabase': 'Folder3','ThirdDatabase': List or single name}

load_openLCA_IPC(port = 8080, project_name="Open_imports",overwrite=False, 
                     nonuser_db_name = 'EcoInvent',check_ecoinvent_exc=False,
                     user_databases={})
                     
example to run function:
import openlca2bw
my_dict = {'FirstDatabase': ['Folder1','Folder2'],'SecondDatabase': 'Folder3'}
#after activating IPC server from OpenLCA
load_openLCA_IPC(project_name="Example_Name",user_databases=my_dict)
                     
                     
OpenLCA database has many exceptions and depending on your database some errors may araise. Feel free to share issues and potential correction.

Next steps for this package :
- Errors and exceptions corrections
- Implementation of function that update a database, rather than uploading the all database
- Implementation of function that import in brightway OpenLCA data from JSON dtabase (.zip)
- Computing and coding optimization
