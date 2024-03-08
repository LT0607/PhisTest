import time

import mysql.connector

#test connection Mysql --> Python
try:

    db = mysql.connector.connect(
        host="localhost",
        user="UserTest",
        password="mdp",
        database="db"
    )

    mySql_Select_Table_Query = "Select * FROM MesureMeteoTest;"

    cursor = db.cursor()
    cursor.execute(mySql_Select_Table_Query)
    print("connection established")
    results = cursor.fetchall()
    for data in results:
        print(data)

except mysql.connector.Error as error:
    print("Failed to select table in MySQL: {}".format(error))
finally:
    if db.is_connected():
        cursor.close()
        db.close()
        print("MySQL connection is closed")
