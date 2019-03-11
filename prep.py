"""
This script compiles on the necessary variables, queries the SQL table and outputs
the DQTBL dataframe

expected input -> config.json
expected output -> DQTBL

DQTBL is created from the CDM.csv files
"""

from .load import Load

import json
import CDMs, pandas
import cx_Oracle as oracle
import psycopg2 as postgresql
import pyodbc as sqlserver

class Prep:
    def __init__(self):
        with open('config.json') as json_file:
            self.config = json.load(json_file)
        
        self.user: str = self.config["Credentials"]["User"]
        self.password: str = self.config["Credentials"]["Password"]
        self.database: str = self.config["DBMS"]

    def Oracle(self):
        conn = oracle.connect(self.user + "/" + self.password + "@" + self.database)

        return Load(conn)

    def PostgreSQL(self):
        host: str = self.config["ConnectionDetails"]["Host"]
        port: str = self.config["ConnectionDetails"]["Port"]
        conn = postgresql.connect(database=self.database,
                                  user=self.user,
                                  password=self.password,
                                  host=host,
                                  port=port)

        return Load(conn)

    def Redshift(self):
        host: str = self.config["ConnectionDetails"]["Host"]
        port: str = self.config["ConnectionDetails"]["Port"]
        conn = postgresql.connect(database=self.database,
                                  user=self.user,
                                  password=self.password,
                                  host=host,
                                  port=port)

        return Load(conn)

    def SQLServer(self):
        driver: str = self.config["ConnectionDetails"]["Driver"]
        server: str = self.config["ConnectionDetails"]["Server"]
        conn = sqlserver.connect("DRIVER=" + driver +
                                 ";SERVER=" + server +
                                 ";DATABASE=" + self.database +
                                 ";UID=" + self.user +
                                 ";PWD=" + self.password)

        return Load(conn)

    def DQTBL(self, CDM: str) -> object:
        return {
            "PCORNET3": pandas.read_csv("./CDMs/DQTBL_pcornet_v3.csv"),
            "PCORNET31": pandas.read_csv("./CDMs/DQTBL_pcornet_v31.csv"),
            "OMOPV5_0": pandas.read_csv("./CDMs/DQTBL_omop_v5_0.csv"),
            "OMOPV5_2": pandas.read_csv("./CDMs/DQTBL_omop_v5_2.csv"),
            "OMOPV5_3": pandas.read_csv("./CDMs/DQTBL_omop_v5_3.csv")
        }[CDM]