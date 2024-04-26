import opensilexClientToolsPython
from pprint import pprint
import datetime
import pytz
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


# Recuperation des données du fichier météo
list_data_meteo = meteo_parser('config.xml')


def day_to_iso_8601(date_text):
    date = parser.parse(date_text)
    return date.astimezone(pytz.timezone("Europe/Paris")).isoformat()


def insert_data(date, variable_uri, value, provenance, target):
    body = [
        opensilexClientToolsPython.DataCreationDTO(date, variable_uri, value, provenance, target=target)]
    try:
        # Add data
        api_response = api_instance.add_list_data(body=body, )
        pprint(api_response)
    except opensilexClientToolsPython.rest.ApiException as e:
        print("Exception when calling DataApi->add_list_data: %s\n" % e)


# Open the JSON file

file = open('data_config.json')
data_config = json.load(file)

# --- récupération des données à insérer selon le fichier de config
for el in list_data_meteo:
    date = day_to_iso_8601(el['date'])
    for k, v in el.items():
        # parcourir les targets
        for i in range(len(data_config['target'])):
            if el['site'] == data_config['target'][i]['name']:
                target_uri = data_config['target'][i]['uri']
                for j in range(len(data_config['target'][i]['variable'])):
                    if data_config['target'][i]['variable'][j]['name'] == k:
                        var_val = v
                        # récupérations de données depuis json
                        var_uri = data_config['target'][i]['variable'][j]['uri']
                        device_uri = data_config['target'][i]['variable'][j]['device']['uri']
                        rdf_type = data_config['target'][i]['variable'][j]['device']['rdf_type']
                        provenance = data_config['target'][i]['variable'][j]['device']['provenance_uri']
                        provenance_used = data_config['target'][i]['variable'][j]['device']['provenance_used']
                        #prov_used = ProvEntityModel(provenance_used, rdf_type)
                        prov_associated_with = ProvEntityModel(device_uri, rdf_type)
                        prov = DataProvenanceModel(provenance, prov_was_associated_with=[prov_associated_with])
                        insert_data(date, var_uri, var_val, prov, target_uri)










