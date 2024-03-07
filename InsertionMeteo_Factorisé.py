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

# --- LIBRARY ---
import os, csv, time, sys, argparse, traceback
from time_module import doy_to_time_array
from utils import *
# from lxml import etree
import xml.etree.ElementTree as ET
from xml.dom import minidom
# --- Parse XML file -----------------------


def file_parser(file):
    parser = argparse.ArgumentParser(description='insertion meteo phenopsis')
    tree = ET.parse(file)
    root = tree.getroot()
    return root


#file_parser('config.xml').findall("lastloc")
# --- Get data from XML file -----------------


def data_file_reader(file, data):
    global data_collected
    alertes = file_parser(file)
    for alerte in alertes:
        data_collected = alerte.find(data).text
    return data_collected

fichierLog = data_file_reader('config.xml', 'log')
print(fichierLog)
# --- Get line at lastloc ---------------------
"""def line_at_lastloc(file):
    numLigne = 1
    lecteur = csv.reader((line.replace('\0', '') for line in file))
    # holds lines already seen
    lignesVues = []
    nbDoublons = 0
    while numLigne < lastloc:
        lecteur.next()
        numLigne = numLigne + 1

    ligneAInserer = 0
    ligneASauter = 0
    for ligne in lecteur:
        ligneASauter += 1
        if ligne == []:  # si ligne vide on passe a la suivante
            continue"""
# --- Read file Insertion_Meteo ---------------

"""def read_file(file, db, site, lastloc):
    if db == 1:
        print("site=", site, "-envoi BD actif", "-lastLoc=", lastloc)
        try:
            meteo = open(file, "r")
        except:
            print("impossible d'ouvrir le fichier meteo")
        nbRow = 0
        for line in meteo:
            nbRow += 1

    meteo.seek(0)  # on repart au debut"""
# --- Variables -------------------------------

"""
fichierLog = data_file_reader('config.xml', 'log') # logs des insertions
lastloc = data_file_reader('config.xml', 'lastloc') # lastLoc
envoiBD = data_file_reader('config.xml', 'bd') # envoi en BD (1: envoi; 0: pas d'envoi)
site = data_file_reader('config.xml', 'site') # site
dataIn = data_file_reader('config.xml', 'meteo') # fichier meteo
email = data_file_reader('config.xml', 'email')  # email(s)

message = time.strftime('%d/%m/%Y %H:%M', time.localtime()) + " - site " + site + "\n"
donneesAInserer = 0
"""