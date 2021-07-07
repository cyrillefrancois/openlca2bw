To install
pip install <path to setup folder>
for exemple C:/user/userprofile/document/openlca2bw

the main functiopn to import olca database to brightway is load_openLCA_IPC()

To import you need to lauch OpenLCA, to open your database and activate the IPC protocol from the OpenLCA software (Tool/Developer tools/IPC server)
The default port value is 8080

The function has default values and can be run without entries, nontheless all processes will be stored in one unique bw2 database ('EcoInvent').
To split OpenLCA into several database you need to specify the database you need to create and the OpenLCA folders related.
The format of user_databases is a dictionnary :

my_dict = {'FirstDatabase': ['Folder1','Folder2',...],'SecondDatabase': 'Folder3','ThirdDatabase': List or single name}

load_openLCA_IPC(port = 8080, project_name="Open_imports",overwrite=False, 
                     nonuser_db_name = 'EcoInvent',check_ecoinvent_exc=False,
                     user_databases={})
