#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 13 11:00:00 2019
Last Modified 18-06-20

- Modification 18-06-20 : prendre en des dates en double :
150,2020,169,614,20.4,66.93,1.314,.792,21.16,412
10254:150,2020,169,614,20.3,69.45,1.314,.727,21.29,412.8
On garde la premiere valeur correspondant a une date donn  e et on saute la seconde

@author: vincent.negre@inrae.fr
"""

### LIBRARY ###
import os, csv, time, sys, argparse, traceback
from time_module import doy_to_time_array
from utils import *
# from lxml import etree
import xml.etree.ElementTree as ET

### RECUPERATION PARAMETRES ###
parser = argparse.ArgumentParser(description='insertion meteo phenopsis')
#parser.add_argument('config.xml','--configfile',  help='config parameters XML file ', required=True)
#args = parser.parse_args()

#configFile = args.configfile

hostBD = 'localhost'
bd = 'db'
login = 'PhenopsisUser'
pwd = 'password'

donneesAInserer = 0
nbInsertions = 0
meteo = ''
#fichierLog = "insertionMeteoLOG1.txt"
#lastloc = 2
#envoiBD = 1
#site = "C1"
#dataIn = "meteo.phenopsis1.dat"
def meteo_parser (config_file):
    ### Parcours fichier XML ###
    global data, dateBD
    list_data = []
    list_date = []
    tree = ET.parse(config_file)
    alertes = tree.getroot()

    for alerte in alertes:
        fichierLog = alerte.find('log').text  # logs des insertions
        lastloc = int(alerte.find('lastloc').text)  # lastLoc
        envoiBD = int(alerte.find('bd').text)  # envoi en BD (1: envoi; 0: pas d'envoi)
        site = alerte.find('site').text  # site
        dataIn = alerte.find("meteo").text  # fichier meteo
        email = alerte.find("mail").text  # email(s)

        message = time.strftime('%d/%m/%Y %H:%M', time.localtime()) + " - site " + site + "\n"
        dataIn = dataIn[22 : ]
        print(dataIn)
        donneesAInserer = 0

        if envoiBD == 1:
            print("site=", site, "-envoi BD actif", "-lastLoc=", lastloc)
            # ---------------------------
            # RECUPERATION DONNEES METEOS
            # ---------------------------
            # Lecture du fichier dataIn
            # -------------------------
            try:
                meteo = open(dataIn, "r")
            except:
                print("impossible d'ouvrir le fichier meteo")

            nbRow = 0
            for line in meteo:
                nbRow += 1

            meteo.seek(0)  # on repart au debut

            # requ  tes SQL d'insertion
            dataToWrite = ""
            fd = ""
            db = ""
            # fichier SQL conteant les requ  tes
            mysqlFile = 'phenopsisC1.sql'
            if lastloc > nbRow:
                message += "aucune donnees a inserer\n"
            else:
                # lecture du fichier
                # pour eviter erreur _csv.Error: line contains NULL byte
                lecteur = csv.reader((line.replace('\0', '') for line in meteo))
                #lecteur = csv.reader(meteo)

                # saut ligne tant qu'on n'a pas atteint le lastloc
                numLigne = 1

                # holds lines already seen
                lignesVues = []
                nbDoublons = 0
                while numLigne < lastloc:
                    next(lecteur)
                    numLigne = numLigne + 1

                ligneAInserer = 0
                ligneASauter = 0
                # print dataIn
                for ligne in lecteur:
                    ligneASauter += 1
                    # print ligne
                    # dateHeure=ligne[1]+"-"+ligne[2]+"-"+ligne[3]
                    if ligne == []:  # si ligne vide on passe a la suivante
                        continue
                    dateHeure = ligne[1] + "-" + ligne[2] + "-" + ligne[3]

                    if dateHeure in lignesVues:  # si ligne en double on passe a la suivante
                        nbDoublons += 1
                        print("doublon" + str(ligne))
                        continue

                    ligneAInserer += 1
                    lignesVues.append(dateHeure)

                    annee = int(ligne[1])
                    jourJulien = int(ligne[2])
                    heure = int(ligne[3])

                    # conversion jourJulien en calendar date (yyyy, mm, dd)
                    date = doy_to_time_array(jourJulien, yyyy=annee)

                    # conversion heure au format hh:mm
                    heure = formatHeure(heure)

                    # attention MySQL pas d'enregistrement si le datetime est AAAA-MM-YY 24:00:00
                    if heure == 2400:
                        jourJulien = jourJulien + 1
                        heure = 00

                        # mise en forme date format BD
                    dateBD = str(date[0]) + "-" + str(date[1]) + "-" + str(date[2]) + " " + str(heure)

                    # stockage des donn  es sous forme de dictionnaire
                    if site == "C1":
                        temperature = float(ligne[4])
                        humidite = float(ligne[5])
                        rayonnement = float(ligne[6])
                        vpd = float(ligne[7])
                        # tp_plafond  = float(ligne[8])
                    elif site == "C2" or site == "C3":
                        temperature = float(ligne[4])
                        humidite = float(ligne[5])
                        rayonnement = float(ligne[6])
                        vpd = float(ligne[7])
                        co2 = float(ligne[9])

                    data = {}

                    if temperature != -6999:
                        data['temperature'] = temperature
                    if humidite != -6999:
                        data['humidite'] = humidite
                    if rayonnement != -6999:
                        data['rayonnement'] = rayonnement
                    if temperature != -6999 and rayonnement != -6999:
                        data['vpd'] = vpd
                    if site == "C2" or site == "C3":
                        data['co2'] = co2
                    list_data.append(data)
                    list_date.append(dateBD)
    return list_data, list_date


