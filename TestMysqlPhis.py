import opensilexClientToolsPython
from pprint import pprint
import datetime
import dateutil.parser as parser
from InsertionMeteoPhis import meteo_parser

from opensilexClientToolsPython import DataProvenanceModel, ProvEntityModel
from opensilexClientToolsPython.rest import ApiException

# test connection Python --> Opensilex
# while True:
pythonClient = opensilexClientToolsPython.ApiClient(verbose=True)
pythonClient.connect_to_opensilex_ws(
    identifier="admin@opensilex.org",
    password="admin",
    host="http://localhost:8666/rest",
)

api_instance = opensilexClientToolsPython.DataApi(pythonClient)

data_structure, date_pheno = meteo_parser('config.xml')


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


var_air_temperature = 'http://opensilex.dev/id/variable/air_temperature_sensor_degree_celsius'
prov_used = ProvEntityModel('dev:id/provenance/temp_sensor_provenance', 'vocabulary:SensingDevice')
prov = DataProvenanceModel('dev:provenance/standard_provenance', [prov_used])
body_u = 'dev:id/data/meteo'
targ = 'dev:id/organization/facility.c1'
time_z = 'Europe/Paris'

#for i, j in zip(data_structure, date_pheno):
#    insertion_data(day_to_iso_8601(j), var_air_temperature, i['temperature'], prov, '', time_z, targ)












