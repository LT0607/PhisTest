import time

import mysql.connector
import opensilexClientToolsPython
from pprint import pprint
from opensilexClientToolsPython.rest import ApiException


#test connection Mysql --> Python
"""
try:
    
    db = mysql.connector.connect(
    host="localhost",
    user="UserTest",
    password="mdp",
    database="db"
    )

    mySql_Select_Table_Query = "Select * FROM MesureMeteoTest;"

    cursor = db.cursor()
    cursor.execute(mySql_Select_Table_Query)
    print("connection established")
    results = cursor.fetchall()
    for data in results:
        print(data)

except mysql.connector.Error as error:
    print("Failed to select table in MySQL: {}".format(error))
finally:
    if db.is_connected():
        cursor.close()
        db.close()
        print("MySQL connection is closed")
"""
#test connection Python --> Opensilex
while True:
    pythonClient = opensilexClientToolsPython.ApiClient(verbose=True)
    pythonClient.connect_to_opensilex_ws(
        identifier="admin@opensilex.org",
        password="admin",
        host="http://localhost:8666/rest",
    )

    api_instance = opensilexClientToolsPython.DevicesApi(pythonClient)

    try:
        # Search devices
        api_response = api_instance.search_devices()
        pprint(api_response)
    except opensilexClientToolsPython.rest.ApiException as e:
        print("Exception when calling DevicesApi->search_devices: %s\n" % e)
    time.sleep(3600)
