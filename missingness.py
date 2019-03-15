"""
expected input: DQTBL
expected output: missingness.csv
"""

import pandas
import datetime
from query import Query

class Missingness:
    def __init__(self, DQTBL: object, query: object):
        # self.DQTBL = pandas.read_csv("./tables/DQTBL.csv")
        self.DQTBL = DQTBL
        self.query = query

        self.missingness = self.DQTBL.apply(self.query.missingnessCalc, axis=1)
        self.missingness.to_csv("reports/missingness.csv")

    # def missingnessFreqCalc(self, row):
    #     nonsense = "'+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI'"
    #     freqQuery = f"""
    #             SELECT COUNT(*)
    #             FROM {self.schema}{row.TabNam} ;"""
    #     uniqFreqQuery = f"""
    #                     SELECT COUNT(DISTINCT {row.ColNam})
    #                     FROM {self.schema}{row.TabNam} ;"""
    #     if (self.DBMS == "sql server" or self.DBMS == "redshift"):
    #         ms1FreqQuery = f"""
    #                     SELECT COUNT('{row.ColNam}')
    #                     FROM {self.schema}{row.TabNam}
    #                     WHERE [{row.ColNam}] IS NULL OR CAST({row.ColNam} AS VARCHAR) IN ("") ;"""
    #         ms2FreqQuery = f"""
    #                     SELECT COUNT('{row.ColNam}')
    #                     FROM {self.schema}{row.TabNam}
    #                     WHERE CAST({row.ColNam} AS VARCHAR) IN ({nonsense}) ;"""
    #     elif self.DBMS == "oracle":
    #         ms1FreqQuery = f"""
    #                     SELECT COUNT('{row.ColNam}')
    #                     FROM {self.schema}{row.TabNam}
    #                     WHERE {row.ColNam} IS NULL OR TO_CHAR({row.ColNam}) IN ("") ;"""
    #         ms2FreqQuery = f"""
    #                     SELECT COUNT('{row.ColNam}')
    #                     FROM {self.schema}{row.TabNam}
    #                     WHERE TO_CHAR({row.ColNam}) IN ({nonsense}) ;"""
    #     elif self.DBMS == "postgresql":
    #         ms1FreqQuery = f"""
    #                     SELECT COUNT('{row.ColNam}')
    #                     FROM {self.schema}{row.TabNam} 
    #                     WHERE '{row.ColNam}' IS NULL OR CAST({row.ColNam} AS VARCHAR) IN ("") ;"""
    #         ms2FreqQuery = f"""
    #                     SELECT COUNT('{row.ColNam}')
    #                     FROM {self.schema}{row.TabNam}
    #                     WHERE CAST({row.ColNam} AS VARCHAR) IN ({nonsense}) ;"""
    #     row["TEST_DATE"] = datetime.datetime.today().strftime('%m-%d-%Y')
    #     row["FREQ"] = pandas.read_sql(freqQuery, con=self.conn).values[0][0]
    #     row["UNIQUE_FREQ"] = pandas.read_sql(uniqFreqQuery, con=self.conn).values[0][0]
    #     row["MS1_FREQ"] = pandas.read_sql(ms1FreqQuery, con=self.conn).values[0][0]
    #     row["MS2_FREQ"] = pandas.read_sql(ms2FreqQuery, con=self.conn).values[0][0]
    #     row["MSs_PERCENTAGE"] = round((row["MS1_FREQ"]+row["MS2_FREQ"])/row["FREQ"], 3)
    #     row["ORGANIZATION"] = self.organization
    #     row["CDM"] = self.CDM
    #     return row