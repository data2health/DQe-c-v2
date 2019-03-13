"""
This script compiles on the necessary variables, queries the SQL table and outputs
the DQTBL dataframe

expected input -> config.json
expected output -> DQTBL

DQTBL is created from the CDM.csv files
"""

import json
import CDMs, pandas
import cx_Oracle as oracle
import psycopg2 as postgresql
import pyodbc as sqlserver

class Prep:
    def __init__(self):
        with open('config.json') as json_file:
            self.config = json.load(json_file)

        self.CDM: str = self.config["CDM"].upper()
        self.DBMS: str = self.config["DBMS"].lower()
        self._user: str = self.config["Credentials"]["User"]
        self._password: str = self.config["Credentials"]["Password"]
        self.database: str = self.config["database"]
        self.host: str = self.config["ConnectionDetails"]["Host"]
        self.port: str = self.config["ConnectionDetails"]["Port"]
        self.schema: str = self.config["schema"]
        self.DQTBL: object = {
                                "PCORNET3": pandas.read_csv("./CDMs/DQTBL_pcornet_v3.csv"),
                                "PCORNET31": pandas.read_csv("./CDMs/DQTBL_pcornet_v31.csv"),
                                "OMOPV5_0": pandas.read_csv("./CDMs/DQTBL_omop_v5_0.csv"),
                                "OMOPV5_2": pandas.read_csv("./CDMs/DQTBL_omop_v5_2.csv"),
                                "OMOPV5_3": pandas.read_csv("./CDMs/DQTBL_omop_v5_3.csv"),
                             }[self.CDM]

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

# ===================================
# self.tableList: object = pandas.DataFrame({ "TabNam":["EMPTY"],
            #                                 "ColNam":["EMPTY"],
            #                                 "NumRows": ["EMPTY"],
            #                                 "Size":["EMPTY"],
            #                                 "Loaded": ["EMPTY"]
            #                               })
# ===================================