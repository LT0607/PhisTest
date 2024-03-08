import opensilexClientToolsPython
from pprint import pprint

from opensilexClientToolsPython import DataProvenanceModel, ProvEntityModel
from opensilexClientToolsPython.rest import ApiException

#test connection Python --> Opensilex
date = '2024-02-18T10:15:30+01:00'
variable_uri = 'http://opensilex.dev/id/variable/plant_weight_image_analysis_milligramme'
value = 19
provenance_used = ProvEntityModel('dev:id/provenance/temp_sensor_provenance', 'vocabulary:SensingDevice')
target = 'dev:id/organization/facility.c1'
provenance = DataProvenanceModel('dev:provenance/standard_provenance', [provenance_used])
timezone = 'Europe/Paris'
body_uri = 'dev:id/data/meteo'
#while True:
pythonClient = opensilexClientToolsPython.ApiClient(verbose=True)
pythonClient.connect_to_opensilex_ws(
    identifier="admin@opensilex.org",
    password="admin",
    host="http://localhost:8666/rest",
)

api_instance = opensilexClientToolsPython.DataApi(pythonClient)

body = [opensilexClientToolsPython.DataCreationDTO(date, variable_uri, value, provenance, body_uri, timezone, target)]
try:
    # Add data
    api_response = api_instance.add_list_data(body=body, )
    pprint(api_response)
except opensilexClientToolsPython.rest.ApiException as e:
    print("Exception when calling DataApi->add_list_data: %s\n" % e)


