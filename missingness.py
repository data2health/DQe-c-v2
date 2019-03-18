"""
expected input: DQTBL
expected output: missingness.csv
"""
from prep import Prep

class Missingness:
    def __init__(self, DQTBL: object, query: object):
        self.DQTBL = DQTBL
        self.query = query
        self.nonsense = "'+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI'"

    ## returns missingness.csv after calculating missingness for each table 
    def get(self):
        if Prep().DBMS == "sql server" or Prep().DBMS == "redshift":
            missingness = self.DQTBL.apply(self.query.missingnessCalc(self.sqlserverRedshiftQuery), axis=1)
        elif Prep().DBMS == "oracle":
            missingness = self.DQTBL.apply(self.query.missingnessCalc(self.oracleQuery), axis=1)
        elif Prep().DBMS == "postgresql":
            missingness = self.DQTBL.apply(self.query.missingnessCalc(self.postgresqlQuery), axis=1)

        return missingness.to_csv("reports/missingness.csv")

    def sqlserverRedshiftQuery(self, row):
        ms1FreqQuery = f"""
                    SELECT COUNT('{row.ColNam}')
                    FROM {Prep().schema}{row.TabNam}
                    WHERE [{row.ColNam}] IS NULL OR CAST({row.ColNam} AS VARCHAR) IN ("") ;"""

        ms2FreqQuery = f"""
                    SELECT COUNT('{row.ColNam}')
                    FROM {Prep().schema}{row.TabNam}
                    WHERE CAST({row.ColNam} AS VARCHAR) IN ({self.nonsense}) ;"""
    
        return ms1FreqQuery, ms2FreqQuery

    def oracleQuery(self, row):
        ms1FreqQuery = f"""
                    SELECT COUNT('{row.ColNam}')
                    FROM {Prep().schema}{row.TabNam}
                    WHERE {row.ColNam} IS NULL OR TO_CHAR({row.ColNam}) IN ("") ;"""

        ms2FreqQuery = f"""
                    SELECT COUNT('{row.ColNam}')
                    FROM {Prep().schema}{row.TabNam}
                    WHERE TO_CHAR({row.ColNam}) IN ({self.nonsense}) ;"""
            
        return ms1FreqQuery, ms2FreqQuery

    def postgresqlQuery(self, row):
        ms1FreqQuery = f"""
                    SELECT COUNT('{row.ColNam}')
                    FROM {Prep().schema}{row.TabNam} 
                    WHERE '{row.ColNam}' IS NULL OR CAST({row.ColNam} AS VARCHAR) IN ("") ;"""
                        
        ms2FreqQuery = f"""
                    SELECT COUNT('{row.ColNam}')
                    FROM {Prep().schema}{row.TabNam}
                    WHERE CAST({row.ColNam} AS VARCHAR) IN ({self.nonsense}) ;"""
            
        return ms1FreqQuery, ms2FreqQuery