import csv
from datetime import datetime
import os
import glob
import shutil
import pandas as pd
from fast_to_sql import fast_to_sql as fts
import pyodbc
import requests
from datetime import date
import configparser
import time

#---------------------------------------------------------------------------
def SQL_conexion (server, database):
    SQLConn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server} ;"
                         "Server=" + server + ";"
                         "Database=" + database + ";"
                         "Trusted_Connection=yes;")
    return SQLConn  #.cursor()
#---------------------------------------------------------------------------
#CONSIGO CONEXION SQL
cp = configparser.ConfigParser()
cp.read("config.ini")
URL = cp["DEFAULT"]["URL"]
Server_Origen = cp["DEFAULT"]["server_origen"]
Base_Origen= cp["DEFAULT"]["base_origen"]

# Se conecta a SQL
Conn = SQL_conexion(Server_Origen, Base_Origen)
cursor = Conn.cursor()
print('Inicio Catalogo Robot : ...',datetime.now().strftime("%H:%M:%S"))

ARCHIVO_CSV = 'PadronRUC_202208.csv' #Este csv se descarga de https://www.datosabiertos.gob.pe/dataset/padr%C3%B3n-ruc-superintendencia-nacional-de-aduanas-y-de-administraci%C3%B3n-tributaria-sunat
BLOQUE_REGISTROS = 100000

ID_ROBOT = 11111116

FECHA_INICIO = datetime.now()
RESULTADO = 1
DESCRIPCION = "Inicio Carga de Contribuyentes Perú"
REGISTROS = 0

#DEFINO LA FECHA DE CORRIDA
FECHA_HORA= datetime.now()
FECHA_HORA = str(FECHA_HORA.strftime("%Y-%m-%d"))

cantRegistros = 0

start = time.time()
    #read data in chunks of 1 million rows at a time
chunk = pd.read_csv(ARCHIVO_CSV,sep=",",encoding='Latin1',low_memory=False,chunksize=1000000)
end = time.time()
print("Read csv with chunks: ",(end-start),"sec")
df = pd.concat(chunk)
print ('Longitud DF ', len(df))

print (df)  

print(df.dtypes)
df["RUC"] = df["RUC"].astype(str)
df["PERIODO_PUBLICACION"] = df["PERIODO_PUBLICACION"].astype(str)


print('Entra al for .....')
for i in range(0, int(len(df) / BLOQUE_REGISTROS) + 1):
        SUMA_INICIO = 0
        INICIO = 0
        FIN = 0
        
        if i > 0:
            SUMA_INICIO = 0
        if (i + 1) * BLOQUE_REGISTROS < len(df):
            INICIO = (i * BLOQUE_REGISTROS) + SUMA_INICIO
            FIN = (i + 1) * BLOQUE_REGISTROS
        else:
            INICIO = (i * BLOQUE_REGISTROS) + SUMA_INICIO
            FIN = len(df)

        ejecuta = fts.fast_to_sql(df[INICIO:FIN],
                                    "dbo.Raw_Contribuyentes",
                                    Conn,
                                    if_exists="append",
                                    temp=False)
        Conn.commit()

        print(INICIO, FIN)
Conn.close()

print(ID_ROBOT, RESULTADO, DESCRIPCION, cantRegistros, FECHA_INICIO)
