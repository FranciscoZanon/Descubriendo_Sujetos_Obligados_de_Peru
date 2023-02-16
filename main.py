from fastapi import FastAPI
import pyodbc
import pandas as pd
import configparser

# CONEXION A BD SQL
#---------------------------------------------------------------------------
def SQL_conexion (server, database):
    SQLConn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server} ;"
                         "Server=" + server + ";"
                         "Database=" + database + ";"
                         "Trusted_Connection=yes;")
    return SQLConn  #.cursor()
#---------------------------------------------------------------------------

app = FastAPI()

@app.get("/")
def pagina_principal():
    return {"Inicio"}

@app.get("/documento/{RUC}")
def sujeto_obligado(RUC):
    #CONSIGO CONEXION SQL
    cp = configparser.ConfigParser()
    cp.read("config.ini")
    URL = cp["DEFAULT"]["URL"]
    Server_Origen = cp["DEFAULT"]["server_origen"]
    Base_Origen= cp["DEFAULT"]["base_origen"]
     # Se conecta a SQL
    Conn = SQL_conexion(Server_Origen, Base_Origen)
    cursor = Conn.cursor()
    SQL_Query = f"SELECT Es_SO from dbo.sujetos_obligados WHERE RUC = '{RUC}'"
    cursor.execute(SQL_Query)
    resultado = cursor.fetchval()
    Conn.close()
    if resultado == 'SI':
        return{'Es sujeto obligado'}
    else:
        return{'No es sujeto obligado'}