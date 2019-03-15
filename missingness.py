"""
expected input: DQTBL
expected output: missingness.csv
"""

import pandas
import datetime

class Missingness:
    def __init__(self, DQTBL: object, query: object):
        self.DQTBL = DQTBL
        self.query = query

    ## returns missingness.csv after calculating missingness for each table 
    def getMissingness(self):
        missingness = self.DQTBL.apply(self.query.missingnessCalc, axis=1)
        
        return missingness.to_csv("reports/missingness.csv")