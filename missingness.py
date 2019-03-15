"""
expected input: DQTBL
expected output: missingness.csv
"""
from prep import Prep

class Missingness:
    def __init__(self, DQTBL: object, query: object):
        self.DQTBL = DQTBL
        self.query = query

    ## returns missingness.csv after calculating missingness for each table 
    def get(self):
        if Prep().DBMS == "sql server" or Prep().DBMS == "redshift":
            missingness = self.DQTBL.apply(self.query.missingnessCalc, axis=1)
        elif Prep().DBMS == "oracle":
            missingness = self.DQTBL.apply(self.query.missingnessCalc, axis=1)
        elif Prep().DBMS == "postgresql":
            missingness = self.DQTBL.apply(self.query.missingnessCalc, axis=1)

        return missingness.to_csv("reports/missingness.csv")

# TODO: rewrite query.missingnessCalc so that it knows which DBMS to use.
#       better to check once here than to check for each row.