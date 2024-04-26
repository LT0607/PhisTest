from datetime import datetime

import mysql.connector
import opensilexClientToolsPython
from Connexion_Opensilex import OpensilexDataInserter, day_to_iso_8601


def transform_idChambre(chambre):
    chambre_mapping = {
        'C1': 'PHENOPSIS-1',
        'C2': 'PHENOPSIS-2',
        'C3': 'PHENOPSIS-3'
    }
    return chambre_mapping.get(chambre, chambre)


def structure_results(rows):
    data_list = []
    #for row in rows:
    facility = transform_idChambre(rows[0])
    date = rows[1]
    variable = rows[2]
    valeur = rows[3]

    data_dict = {
        'facility': facility,
        'date': date,
        'variable': variable,
        'valeur': valeur
    }

    data_list.append(data_dict)

    return data_list


def fetch_data_from_mysql(batch_size=100):
    try:
        # Connexion à la base de données MySQL
        db = mysql.connector.connect(
            host="localhost",
            user="UserTest",
            password="Password",
            database="phenopsis_test"
        )
        cursor = db.cursor()

        # Création de la requête SQL
        mySql_Select_Table_Query = "SELECT idChambre, date, idVariable, valeur FROM MesureMeteo;"

        # Exécution de la requête SQL
        cursor.execute(mySql_Select_Table_Query)
        print("Connection established")

        # Récupération de toutes les lignes avec un générateur
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                yield row

    except mysql.connector.Error as error:
        print("Failed to select table in MySQL: {}".format(error))

    finally:
        # Fermeture du curseur et de la connexion
        try:
            cursor.close()
        except:
            pass
        try:
            db.close()
        except:
            pass
        print("MySQL connection is closed")


def insert_data_to_opensilex(inserter, dataToInsert):
    data_config = inserter.load_data_config(inserter.opensilex_config_path)

    for element in dataToInsert:
        date = element['date']
        for i in range(len(data_config['target'])):
            if element['facility'] == data_config['target'][i]['name']:
                target_uri = data_config['target'][i]['uri']
                for j in range(len(data_config['target'][i]['variable'])):
                    if element['variable'] == data_config['target'][i]['variable'][j]['name']:
                        var_val = element['valeur']
                        var_uri = data_config['target'][i]['variable'][j]['uri']
                        device_uri = data_config['target'][i]['variable'][j]['device']['uri']
                        rdf_type = data_config['target'][i]['variable'][j]['device']['rdf_type']
                        provenance = data_config['target'][i]['variable'][j]['device']['provenance_uri']
                        provenance_used = data_config['target'][i]['variable'][j]['device']['provenance_used']
                        prov_associated_with = opensilexClientToolsPython.ProvEntityModel(device_uri, rdf_type)
                        prov = opensilexClientToolsPython.DataProvenanceModel(provenance, prov_was_associated_with=[prov_associated_with])
                        inserter.insert_data(date, var_uri, var_val, prov, target_uri)


# Ainsi, dans votre code principal, vous pouvez appeler cette fonction en passant l'objet inserter comme argument :
if __name__ == "__main__":
    inserter = OpensilexDataInserter("data_config.json")
    inserter.connect_to_opensilex("admin@opensilex.org", "admin", "http://localhost:8666/rest")

    data_generator = fetch_data_from_mysql()

    for row in data_generator:
        structured_data = structure_results(row)
        insert_data_to_opensilex(inserter, structured_data)

