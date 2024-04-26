import opensilexClientToolsPython
from pprint import pprint
import pytz
import dateutil.parser as parser
import json


def day_to_iso_8601(self, date_text):
    date = parser.parse(date_text)
    return date.astimezone(pytz.timezone("Europe/Paris")).isoformat()


class OpensilexDataInserter:
    def __init__(self, opensilex_config_path):
        self.pythonClient = opensilexClientToolsPython.ApiClient(verbose=True)
        self.opensilex_config_path = opensilex_config_path

    def connect_to_opensilex(self, identifier, password, host):
        self.pythonClient.connect_to_opensilex_ws(identifier=identifier, password=password, host=host)

    def insert_data(self, date, variable_uri, value, provenance, target):
        api_instance = opensilexClientToolsPython.DataApi(self.pythonClient)
        body = [opensilexClientToolsPython.DataCreationDTO(date, variable_uri, value, provenance, target=target)]
        try:
            api_response = api_instance.add_list_data(body=body)
            pprint(api_response)
        except opensilexClientToolsPython.rest.ApiException as e:
            print("Exception when calling DataApi->add_list_data: %s\n" % e)

    def load_data_config(self, config_path):
        with open(config_path) as file:
            return json.load(file)




