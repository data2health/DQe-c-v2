"""
expected input: not sure
expected output: indicators.csv
"""
from prep import Prep
import pandas, datetime

class Indicator:
    def __init__(self, DQTBL: object, query: object):
        self.DQTBL = DQTBL
        self.query = query
        self.organization = Prep().organization
        self.CDM = Prep().CDM
        self.conn = Prep().conn

    ## returns missingness.csv after calculating missingness for each table 
    def get(self):
        # indicators = self.DQTBL.apply(self.query.missingnessCalc, axis=1)
        pass
        # return indicators.to_csv("reports/indicators.csv")

    def queryInd(self):
        indicator = pandas.DataFrame()

        indicator["GROUP"] = pandas.read_sql("SQL query", con=self.conn).values[0][0]
        indicator["MISSING PERCENTAGE"] = pandas.read_sql("SQL query", con=self.conn).values[0][0]
        indicator["MISSING POPULATION"] = pandas.read_sql("SQL query", con=self.conn).values[0][0]
        indicator["DENOMINATOR"] = pandas.read_sql("SQL query", con=self.conn).values[0][0]
        indicator["PERCENTAGE"] = []
        indicator["TEST_DATE"] = datetime.datetime.today().strftime('%m-%d-%Y')
        indicator["ORGANIZATION"] = self.organization
        indicator["CDM"] = self.CDM

        return indicator