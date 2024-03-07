# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 10:07:50 2013

@author: vincent
"""
from math import exp
from decimal import Decimal, getcontext
import MySQLdb
import smtplib
import os,sys, traceback
from email.mime.text import MIMEText

def nblignes(nf, fdl='\n', tbuf=16384):
    """Compte le nombre de lignes du fichier nf"""
    c = 0
    f = open(nf, 'rb')
    while True:
        buf = None
        buf = f.read(tbuf)
        if len(buf)==0:
            break
        c += buf.count(fdl)
    f.seek(-1, 2)
    #car = f.read(1)
    #if car != fdl:
    #    c += 1
    f.close()
    return c 

def formatHeure(heure):
            """ convertit heure au format hh:mm

            Inputs:
            1. heure:<integer>: heure non formatte (0,15,100, 115, ...)

            Returns:
            ---------------------------------------------
            1. yyyy: <string>: heure au format hh:mm

            """
            heure = str(heure);
            listHeure = list(heure);

            #if int(heure) == 0:
            #	return "00:00"
            #elif int(heure) == 5:
            #	return "00:05"
            if len(heure) == 1:
                return "00:0" + listHeure[0]
            elif len(heure) == 2:#heure au format 15, 30, 45 ...
                return "00" + ":" + listHeure[0] + listHeure[1]
            elif len(heure)== 3:#heure au format 115, 130, 145 ...
                return "0" + listHeure[0] + ":" + listHeure[1] + listHeure[2]
            elif len(heure)== 4:
                return listHeure[0] + listHeure[1] +  ":" + listHeure[2] + listHeure[3]
            else:
                return
        
def calculVPD(temperature,humidite):
    """ calcule le VPD
    
    Inputs:
    1. temperature:<float>: temperature en degres celcius
    2. humidite:<float>: humidite relative en %

    Returns:
    --------------------------------------------
    1. vpd: <float>: vpd

    """
    vpd = 0.611*exp((17.27*temperature)/(temperature+237.3))*(1-(humidite/100))
    return vpd;
    
def arrondi(nombre,nb_apres_virgule):
    """ arrondi un nombre decimal
    
    Inputs:
    1. nombre:<float>: nombre a arrondir
    2. nb_apres_virgule:<int>: nombre de decimales

    Returns:
    --------------------------------------------
    1. nombre_arrondi: <float>: nombre arrondi

    """
    getcontext().prec = nb_apres_virgule; #precision voulue
    return Decimal(str(nombre));
    
def BDD_connect(host,bd,login,pwd):
    """ connexion a la bd
    
    Inputs:
    1. host: <string>: adresse du serveur
    2. bd: <string>: nom base de donnees
    3. login: <string> : login
    4. pwd:<string> :  mot de passe
    
    
    Returns:
    -------------------------------------------
    1. db: <MySQLdb.connections.Connection>: instance de connexion à la BD
    """
    
    try:
        db = MySQLdb.connect(host=host, \
            user=login, \
            passwd=pwd, \
            db=bd);
        db.autocommit(False)
        return db;
    except:
        print("Impossible de se connecter a la base de donnees\n");
        return 0;


def BDD_insertFile(db, sqlfile):
    #Creation d'un curseur sur la BDD
    cur = db.cursor()
    nbinsert = 0
    ligne = ""
    try:
        fd = open(sqlfile, 'r')
        lignes = fd.readlines()
        fd.close()
    except Exception as e:
        print("Impossible de reouvrir le fichier d'insertion\n")
        print(e)
        return 0
    try:
        #cur.execute("START TRANSACTION")
        for ligne in lignes:
            cur.execute(ligne)
            nbinsert=nbinsert+1
        db.commit()
    except MySQLdb.Error as e:
        #cur.execute("ROLLBACK")
        db.rollback()
        print("Probleme dans l'execution de la requete d'insertion")
        print("requete=", ligne)
        #print sys.exc_info()
        print(e.args[0], e.args[1])
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        message = traceback.format_exc()
        print("MESSAGE=", message)
        #return 0
    cur.close()
    return nbinsert
    
def envoiMail(destinataire,message,subject):
    # Create a text/plain message    
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From']    = 'noreply@supagro.inra.fr'
    msg['To']      = destinataire
    
    # Send the message via local SMTP server.
    s = smtplib.SMTP('smtp.supagro.inra.fr')
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    s.sendmail('noreply@supagro.inra.fr', destinataire, msg.as_string())
    s.quit()

def ecriture_fichierLog(fichierLog, message):
    if fichierLog != None:
        #ecriture des logs
        try:
           fd = open(fichierLog, "a")
           fd.write("\n"+message)
        except:
            print("Impossible de creer le fichier d'insertion " + fichierLog + "\n")
        finally:
            fd.close()
    else:
        print(message)
    
