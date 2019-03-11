"""
Loads the DQTBL dataframe from prep
compares the expected tables from the CDM csv files to the actual tables in the SQL database


expected input: DQTBL
expected output: tablelist.csv
"""
import pandas

class Load:
    def __init__(self, connection, DQTBL: object):
        self.conn = connection
        self.DQTBL = DQTBL
        self.payload: dict = { "Table Name":["EMPTY"],
                               "Column Name":["EMPTY"],
                               "Number of Rows": ["EMPTY"],
                               "Total Size":["EMPTY"],
                               "Loaded": ["EMPTY"]
                             }
        tableList = pandas.DataFrame(data=payload)