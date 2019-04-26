"""
expected input: DQTBL
expected output: missingness.csv
"""
import datetime
import pandas

class Missingness:
    def __init__(self, query: object):
        self.query = query
        self.nonsense = "'+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI'"

    ## returns missingness.csv after calculating missingness for each table 
    def missing(self):
        if self.query.DBMS == "sql server" or self.query.DBMS == "redshift":
            missingness = self.query.DQTBL.apply(lambda row: self.missingnessCalc(row, self.sqlserverRedshiftQuery), axis=1)
        elif self.query.DBMS == "oracle":
            missingness = self.query.DQTBL.apply(lambda row: self.missingnessCalc(row, self.oracleQuery), axis=1)
        elif self.query.DBMS == "postgresql":
            missingness = self.query.DQTBL.apply(lambda row: self.missingnessCalc(row, self.postgresqlQuery), axis=1)

        self.query.DQTBL = missingness


    def sqlserverRedshiftQuery(self, row):
        ms1FreqQuery = f"""
                    SELECT COUNT('{row.ColNam}')
                    FROM {self.query.prefix}{row.TabNam}
                    WHERE [{row.ColNam}] IS NULL OR CAST({row.ColNam} AS VARCHAR) IN ('') ;"""

        ms2FreqQuery = f"""
                    SELECT COUNT('{row.ColNam}')
                    FROM {self.query.prefix}{row.TabNam}
                    WHERE CAST({row.ColNam} AS VARCHAR) IN ({self.nonsense}) ;"""
    
        return ms1FreqQuery, ms2FreqQuery


    def oracleQuery(self, row):
        ms1FreqQuery = f"""
                    SELECT COUNT('{row.ColNam}')
                    FROM {self.query.prefix}{row.TabNam}
                    WHERE {row.ColNam} IS NULL OR TO_CHAR({row.ColNam}) IN ("") ;"""

        ms2FreqQuery = f"""
                    SELECT COUNT('{row.ColNam}')
                    FROM {self.query.prefix}{row.TabNam}
                    WHERE TO_CHAR({row.ColNam}) IN ({self.nonsense}) ;"""
            
        return ms1FreqQuery, ms2FreqQuery


    def postgresqlQuery(self, row):
        ms1FreqQuery = f"""
                    SELECT COUNT('{row.ColNam}')
                    FROM {self.query.prefix}{row.TabNam} 
                    WHERE '{row.ColNam}' IS NULL OR CAST({row.ColNam} AS VARCHAR) IN ('');"""
                        
        ms2FreqQuery = f"""
                    SELECT COUNT('{row.ColNam}')
                    FROM {self.query.prefix}{row.TabNam}
                    WHERE CAST({row.ColNam} AS VARCHAR) IN ({self.nonsense}) ;"""
            
        return ms1FreqQuery, ms2FreqQuery


    
    
    def missingnessCalc(self, row, func):
        freqQuery = f"""
                SELECT COUNT(*)
                FROM {self.query.prefix}{row.TabNam} ;"""

        uniqFreqQuery = f"""
                SELECT COUNT(DISTINCT {row.ColNam})
                FROM {self.query.prefix}{row.TabNam} ;"""

        ms1FreqQuery, ms2FreqQuery = func(row)

        row["TEST_DATE"] = datetime.datetime.today().strftime('%m-%d-%Y')
        #row["FREQ"] = pandas.read_sql(freqQuery, con=self.conn).values[0][0]
        row["UNIQUE_FREQ"] = pandas.read_sql(uniqFreqQuery, con=self.query.conn).values[0][0]
        row["MS1_FREQ"] = pandas.read_sql(ms1FreqQuery, con=self.query.conn).values[0][0]
        row["MS2_FREQ"] = pandas.read_sql(ms2FreqQuery, con=self.query.conn).values[0][0]

        try:
            row["MSs_PERCENTAGE"] = round((row["MS1_FREQ"]+row["MS2_FREQ"])/row["Rows"], 3)
        except ZeroDivisionError:
            row["MSs_PERCENTAGE"] = round(0, 3)
            
        row["ORGANIZATION"] = self.query.organization
        row["CDM"] = self.query.CDM

        return row