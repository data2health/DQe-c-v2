"""
Loads the DQTBL dataframe from prep
compares the expected tables from the CDM csv files to the actual tables in the SQL database


expected input: DQTBL
expected output: tablelist.csv
"""

class Load:
    def __init__(self, connection, DQTBL):
        self.conn = connection
        self.DQTBL = DQTBL