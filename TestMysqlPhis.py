import opensilexClientToolsPython
from pprint import pprint
import datetime
import dateutil.parser as parser
from InsertionMeteoPhis import meteo_parser

from opensilexClientToolsPython import DataProvenanceModel, ProvEntityModel
from opensilexClientToolsPython.rest import ApiException

import json

# -------- test connection Python --> Opensilex -----------------------------------
# while True:
pythonClient = opensilexClientToolsPython.ApiClient(verbose=True)
pythonClient.connect_to_opensilex_ws(
    identifier="admin@opensilex.org",
    password="admin",
    host="http://localhost:8666/rest",
)

api_instance = opensilexClientToolsPython.DataApi(pythonClient)

# Open the JSON file

file = open('data_config.json')
data_config = json.load(file)


# Recuperation des données du fichier météo
list_data_meteo = meteo_parser('config.xml')
#print(list_data_meteo)

def day_to_iso_8601(date_text):
    date = parser.parse(date_text)
    return date.astimezone().isoformat()


def insertion_data(date, variable_uri, value, provenance, body_uri, timezone, target):
    body = [
        opensilexClientToolsPython.DataCreationDTO(date, variable_uri, value, provenance, body_uri, timezone, target)]
    try:
        # Add data
        api_response = api_instance.add_list_data(body=body, )
        pprint(api_response)
    except opensilexClientToolsPython.rest.ApiException as e:
        print("Exception when calling DataApi->add_list_data: %s\n" % e)

#--- récupération des données à insérer selon le fichier de config
for el in list_data_meteo:
    date = day_to_iso_8601(el['date'])
    for k, v in el.items():
        for i in range(3):
            if el['site'] == data_config['target'][i]['name']:
                target_uri = data_config['target'][i]['uri']
                for j in range(len(data_config['target'][i]['variable'])):
                    if data_config['target'][i]['variable'][j]['name'] == k:
                        var_uri = data_config['target'][i]['variable'][j]['uri']
                        device_uri = data_config['target'][i]['variable'][j]['device']['uri']


#date = day_to_iso_8601(el['date'])

    #print(list(el.keys())[list(el.values()).index(el['temperature'])])



    #for var in list_data_meteo['variable']:
     #   var['name']


"""def insertion_air_temperature():
    variable = 'http://opensilex.dev/id/variable/air_temperature_sensor_degree_celsius'
    prov_used = ProvEntityModel('dev:id/provenance/temp_sensor_provenance', 'vocabulary:SensingDevice')
    prov = DataProvenanceModel('dev:provenance/standard_provenance', [prov_used])
    for element in list_data:
        insertion_data(day_to_iso_8601(element['date']), variable, element['temperature'], prov, body_u, time_z, element['site'])"""

# -------- Main ----------------------------------------------------------









