import opensilexClientToolsPython
from Connexion_Opensilex import OpensilexDataInserter  # Import de la classe depuis le premier script
from InsertionMeteoPhis import meteo_parser


def run():
    inserter = OpensilexDataInserter("data_config.json")  # Création d'une instance de la classe du premier script
    inserter.connect_to_opensilex("admin@opensilex.org", "admin", "http://localhost:8666/rest")

    list_data_meteo = meteo_parser('config.xml')
    data_config = inserter.load_data_config(inserter.opensilex_config_path)

    for el in list_data_meteo:
        date = inserter.day_to_iso_8601(el['date'])
        for k, v in el.items():
            for i in range(len(data_config['target'])):
                if el['site'] == data_config['target'][i]['name']:
                    target_uri = data_config['target'][i]['uri']
                    for j in range(len(data_config['target'][i]['variable'])):
                        if data_config['target'][i]['variable'][j]['name'] == k:
                            var_val = v
                            var_uri = data_config['target'][i]['variable'][j]['uri']
                            device_uri = data_config['target'][i]['variable'][j]['device']['uri']
                            rdf_type = data_config['target'][i]['variable'][j]['device']['rdf_type']
                            provenance = data_config['target'][i]['variable'][j]['device']['provenance_uri']
                            provenance_used = data_config['target'][i]['variable'][j]['device']['provenance_used']
                            prov_associated_with = opensilexClientToolsPython.ProvEntityModel(device_uri, rdf_type)
                            prov = opensilexClientToolsPython.DataProvenanceModel(provenance, prov_was_associated_with=[prov_associated_with])
                            inserter.insert_data(date, var_uri, var_val, prov, target_uri)


if __name__ == "__main__":
    # Exécution de la fonction run uniquement si c'est le script principal
    run()
