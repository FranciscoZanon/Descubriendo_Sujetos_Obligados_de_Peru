import pyodbc
from datetime import datetime
from datetime import date
from ast import If
import configparser
from re import T
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys
import pandas as pd
from fast_to_sql import fast_to_sql as fts



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
# Configura Chromedriver
options = webdriver.ChromeOptions()
options.headless = False
preferences = { "download.directory_upgrade": True,
                "safebrowsing_for_trusted_sources_enabled": False,
                "safebrowsing.enabled": False,
                "profile.default_content_setting_values.automatic_downloads": 1,
                "download.prompt_for_download": False }
options.add_argument("--ignore-certificate-errors")
options.add_experimental_option("prefs", preferences)
options.add_experimental_option("excludeSwitches", ['enable-automation'])
options.add_argument('--kiosk-printing')
options.add_argument('--disable-gpu')
options.add_argument('--disable-software-rasterizer')
options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)
driver = webdriver.Chrome(options=options)

# Main
driver.get('https://www.sbs.gob.pe/app/uif/voc/')
WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,'//*[@id="btnBuscar"]')))
Sujetos = pd.DataFrame (columns=['RUC','Es_SO','Fecha_Proceso'])
respuesta = "inicio"

while len(respuesta) > 0:
    try:
        # Agrega contribuyentes al DF (dataframe) en bloques de 15
        cursor.execute("SELECT TOP 15 r.RUC FROM Raw_Contribuyentes r left JOIN dbo.sujetos_obligados so on r.RUC = so.RUC where so.Es_SO IS NULL")
        respuesta = cursor.fetchall()
        for fila in respuesta:
            RUC = fila[0]
            searchbox = driver.find_element(by=By.XPATH, value='//*[@id="txtRuc"]')
            searchbox.click()
            time.sleep(2)
            searchbox.send_keys(f"{RUC}")
            time.sleep(2)
            buscar = driver.find_element(by=By.XPATH, value='//*[@id="btnBuscar"]')
            buscar.click()
            try:
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,'//*[@id="tblResultado"]/tbody/tr[2]')))
                element = driver.find_element(by=By.XPATH, value='//*[@id="tblResultado"]/tbody/tr[2]')
                fila = element.find_elements(by=By.TAG_NAME, value="td")
                print(f"{RUC}",fila[2].text)
                FECHA = datetime.now()
                Sujetos = pd.concat([Sujetos, pd.Series ( [RUC,'SI',FECHA],index = Sujetos.columns).to_frame().T])
                time.sleep(3)
                driver.get('https://www.sbs.gob.pe/app/uif/voc/')
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,'//*[@id="btnBuscar"]')))
            except:
                print(f"{RUC} No está registrado en UIF-Perú")
                FECHA = datetime.now()
                Sujetos = pd.concat([Sujetos, pd.Series ( [RUC,'NO',FECHA],index = Sujetos.columns).to_frame().T])
                driver.get('https://www.sbs.gob.pe/app/uif/voc/')
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,'//*[@id="btnBuscar"]')))
    
    # Primera pasada (todavía no existe la tabla dbo.sujetos_obligados)
    except:
        cursor.execute("SELECT TOP 15 RUC FROM Raw_Contribuyentes")
        respuesta = cursor.fetchall()
        for fila in respuesta:
            RUC = fila[0]
            searchbox = driver.find_element(by=By.XPATH, value='//*[@id="txtRuc"]')
            searchbox.click()
            time.sleep(2)
            searchbox.send_keys(f"{RUC}")
            time.sleep(2)
            buscar = driver.find_element(by=By.XPATH, value='//*[@id="btnBuscar"]')
            buscar.click()
            try:
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,'//*[@id="tblResultado"]/tbody/tr[2]')))
                element = driver.find_element(by=By.XPATH, value='//*[@id="tblResultado"]/tbody/tr[2]')
                fila = element.find_elements(by=By.TAG_NAME, value="td")
                print(f"{RUC}",fila[2].text)
                FECHA = datetime.now()
                Sujetos = pd.concat([Sujetos, pd.Series ( [RUC,'SI',FECHA],index = Sujetos.columns).to_frame().T])
                time.sleep(3)
                driver.get('https://www.sbs.gob.pe/app/uif/voc/')
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,'//*[@id="btnBuscar"]')))
            except:
                print(f"{RUC} No está registrado en UIF-Perú")
                FECHA = datetime.now()
                Sujetos = pd.concat([Sujetos, pd.Series ( [RUC,'NO',FECHA],index = Sujetos.columns).to_frame().T])
                driver.get('https://www.sbs.gob.pe/app/uif/voc/')
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,'//*[@id="btnBuscar"]')))

    df = Sujetos    
    create_statement = fts.fast_to_sql(df, 'dbo.sujetos_obligados', Conn, if_exists='replace')
    Conn.commit()
Conn.close()