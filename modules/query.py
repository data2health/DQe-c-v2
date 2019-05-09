import json
import pandas

try:
    import cx_Oracle as oracle
except ModuleNotFoundError:
    import cx_oracle as oracle

import psycopg2 as postgresql
import pyodbc as sqlserver
import datetime
from typing import Dict, List
import json
import os

class Query:
    def __init__(self, config_file='config.json'):
        with open(config_file) as json_file:
            self.config = json.load(json_file)

        self.CDM: str = self.config["CDM"].upper()
        if self.CDM == "":
            raise NameError(f"A Common Data Model (CDM) has not been defined in {config_file}")
        elif self.CDM not in ["PCORNET3","PCORNET31","OMOPV5_0","OMOPV5_2","OMOPV5_3"]:
            raise NameError(f"{self.CDM} is not a valid Common Data Model")


        self.organization: str = self.config["Organization"]
        self._user: str = self.config["Credentials"]["User"]
        self._password: str = self.config["Credentials"]["Password"]
        self.database: str = self.config["database"]
        self.host: str = self.config["ConnectionDetails"]["Host"]
        self.port: str = self.config["ConnectionDetails"]["Port"]
        self.schema: str = self.config["schema"]
        self.vocab_schema: str = self.config["vocabulary schema"]

        self.DQTBL: object = {
                                "PCORNET3": pandas.read_csv("./CDMs/DQTBL_pcornet_v3.csv"),
                                "PCORNET31": pandas.read_csv("./CDMs/DQTBL_pcornet_v31.csv"),
                                "OMOPV5_0": pandas.read_csv("./CDMs/DQTBL_omop_v5_0.csv"),
                                "OMOPV5_2": pandas.read_csv("./CDMs/DQTBL_omop_v5_2.csv"),
                                "OMOPV5_3": pandas.read_csv("./CDMs/DQTBL_omop_v5_3.csv")
                                ## Add new common data models here
                             }[self.CDM]

        self.DBMS: str = self.config["DBMS"].lower()

        if self.DBMS == "oracle":
            self.conn = self.Oracle()
        elif self.DBMS == "postgresql":
            self.conn = self.PostgreSQL()
        elif self.DBMS == "redshift":
            self.conn = self.Redshift()
        elif self.DBMS == "sql server":
            self.conn = self.SQLServer()
        elif self.DBMS == "":
            raise NameError("No DBMS defined in config.json")
        else:
            raise NameError("'%s' is not an accepted DBMS" % self.DBMS)


        if self.schema != "":
            self.prefix = self.database + "." + self.schema + "."
            self.query_prefix = f"'{self.schema}.' ||"
        else:
            self.prefix = ""
            self.query_prefix = ""


        if self.vocab_schema != "":
            self.vocab_prefix = self.database + "." + self.vocab_schema + "."
        else:
            self.vocab_prefix = ""


    def Oracle(self):
        conn = oracle.connect(self._user + "/" + self._password + "@" + self.database)

        return conn


    def PostgreSQL(self):
        conn = postgresql.connect(database=self.database,
                                  user=self._user,
                                  password=self._password,
                                  host=self.host,
                                  port=self.port)
        return conn


    def Redshift(self):
        conn = postgresql.connect(database=self.database,
                                  user=self._user,
                                  password=self._password,
                                  host=self.host,
                                  port=self.port)
        return conn


    def SQLServer(self):
        driver: str = self.config["ConnectionDetails"]["Driver"]
        conn = sqlserver.connect("DRIVER=" + driver +
                                 ";SERVER=" + self.host +
                                 ";DATABASE=" + self.database +
                                 ";UID=" + self._user +
                                 ";PWD=" + self._password)
        return conn

    
    def outputReport(self, report_df, report_filename):
        
        reportFolder = f"./reports/{self.organization}/{datetime.datetime.today().strftime('%m-%d-%Y')}/"
        if not (os.path.exists(reportFolder)):
            os.makedirs(reportFolder)
        
        report_df.to_csv(f"{reportFolder}/{report_filename}", index=False)

    
 
