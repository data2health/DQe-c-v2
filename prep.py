"""
This script compiles on the necessary variables, queries the SQL table and outputs
the DQTBL dataframe

expected input -> config.json
expected output -> DQTBL

DQTBL is created from the CDM.csv files
"""

from .load import Load

import json
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

# TODO: How are the CDMs .csv files loaded? Is it based on the databases? or are they separate?
#       E.g., is OMOP for a specific database?