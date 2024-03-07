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
#espaceTravail = "/home/phenopsis/METEO";
#os.chdir(espaceTravail);
fichierLog = "insertionMeteoLOG1.txt"
lastloc = 2
envoiBD = 1
site = "C1"
dataIn = "meteo.phenopsis1.dat"

message = time.strftime('%d/%m/%Y %H:%M', time.localtime()) + " - site " + site + "\n"

donneesAInserer = 0
### Parcours fichier XML ###
tree = ET.parse('config.xml')
alertes = tree.getroot()

"""for alerte in alertes:
    fichierLog = alerte.find('log').text  # logs des insertions
    lastloc = int(alerte.find('lastloc').text)  # lastLoc
    envoiBD = int(alerte.find('bd').text)  # envoi en BD (1: envoi; 0: pas d'envoi)
    site = alerte.find('site').text  # site
    dataIn = alerte.find("meteo").text  # fichier meteo
    email = alerte.find("mail").text  # email(s)"""

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
            elif site == "B1":
                temperature = float(ligne[5])
                humidite = float(ligne[6])
                rayonnement = float(ligne[41])
                vpd = round(calculVPD(temperature, humidite), 3)
            elif site == "B2":
                temperature = float(ligne[7])
                humidite = float(ligne[8])
                rayonnement = float(ligne[42])
                vpd = round(calculVPD(temperature, humidite), 3)
            elif site == "B3":
                temperature = float(ligne[9])
                humidite = float(ligne[10])
                rayonnement = float(ligne[43])
                vpd = round(calculVPD(temperature, humidite), 3)
            elif site == "B4":
                temperature = float(ligne[11])
                humidite = float(ligne[12])
                rayonnement = float(ligne[44])
                vpd = round(calculVPD(temperature, humidite), 3)
            elif site == "B5":
                temperature = float(ligne[13])
                humidite = float(ligne[14])
                rayonnement = float(ligne[45])
                vpd = round(calculVPD(temperature, humidite), 3)
            elif site == "B6":
                temperature = float(ligne[15])
                humidite = float(ligne[16])
                rayonnement = float(ligne[46])
                vpd = round(calculVPD(temperature, humidite), 3)
            elif site == "BH":
                temperature = float(ligne[6])
                humidite = float(ligne[5])
                rayonnement = float(ligne[7])
                vpd = round(calculVPD(temperature, humidite), 3)

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

            # parcours des donn  es pour generation requete SQL
            for var in data:
                donneesAInserer += 1
                sql = "INSERT INTO MesureMeteoTest (idChambre,date,idVariable,valeur) VALUES ('" +site + "','" + dateBD + "','" + var + "'," + str(data[var]) + ");\n"
                dataToWrite += sql

        # enregistrement lignes SQL dans fichier
        try:
            fd = open(mysqlFile, 'w')
            fd.write(dataToWrite)
        except:
            print("Impossible de creer le fichier d'insertion " + mysqlFile + "\n")
        finally:
            fd.close()
            # exit

        # connection    la base
        try:
            db = BDD_connect(host=hostBD, bd=bd, login=login, pwd=pwd)
        except:
            print("Impossible de se connecter a la bd\n")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            message = traceback.format_exc()
            print(message)

        # insertion du fichier SQL
        try:
            nbInsertions = BDD_insertFile(db, mysqlFile)
            # nbInsertions=donneesAInserer #pour test sans insertion en BD
        except:
            print("probleme insertion SQL\n")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            message = traceback.format_exc()
            print(message)

        message += "nombre de lignes a inserer:" + str(ligneAInserer) + "\n"
        message += "nombre d'enregistrements a inserer:" + str(donneesAInserer) + "\n"
        message += "nombre d'enregistrements insere:" + str(nbInsertions) + "\n"
        message += "nombre de lignes en double:" + str(nbDoublons) + "\n"

        if nbInsertions != donneesAInserer:
            message += "pas de MAJ du lastloc" + "\n"
        """else:
            # MAJ du lastloc
            lastlocNew = lastloc + ligneASauter
            print("nouveau lastLoc:" + str(lastlocNew) + "\n")
            message += "nouveau lastLoc:" + str(lastlocNew) + "\n"
            try:
                alerte.find('lastloc').text = str(lastlocNew)
                print("lastloc MAJ\n")
            except:
                print("impossible de mettre    jour le lastloc\n")
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                message = traceback.format_exc()
                print(message)"""
    meteo.close()
else:
    message += "pas d'envoi en BD active\n"

# envoi logs
# if email != None:
#    for destinataire in email.split(';'):
#        #print "mail-",destinataire
#        subject = "insertion meteo " + site
#        envoiMail(destinataire,message,subject)
# ecriture Log
ecriture_fichierLog("writeLogTest", message)
# if fichierLog != None:
#    #ecriture des logs
#    try:
#       fd = open(fichierLog, "a")
#       fd.write("\n"+message)
#    except:
#        print "Impossible de creer le fichier d'insertion " + fichierLog + "\n"
#    finally:
#        fd.close()
#else:
#    print message

#MAJ fichier XML
#tree.write(configFile)
