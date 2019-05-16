"""
computes the orphan keys in a few main tables
"""
import pandas
import json

class Orphan:
    def __init__(self, query: object):
        # ==================================
        # Initiates the orphan class
        # creates the DQTBL object
        # ==================================

        self.query = query
        self.DQTBL = query.DQTBL


    def getRefPrime(self):
        
        ## gather primary keys from DQTBL
        primaryPairs: object = self.DQTBL[(self.DQTBL["primary"]) & (self.DQTBL["cat"].isin(["clinical", "health_system"]))][["ColNam", "TabNam"]]
        
        ## gather all non-primary table-column pairs
        referencePairs: object = self.DQTBL[(self.DQTBL["primary"] == False) & (self.DQTBL["cat"].isin(["clinical", "health_system"]))][["ColNam", "TabNam"]]

        ## merge primary and reference tables and remove all non-reference keys
        referencePrimaryMerge: object = primaryPairs.merge(referencePairs, on="ColNam", how="right", suffixes=("_primary", "_external"))
        referencePrimaryMerge.dropna(subset = ["TabNam_primary"], inplace=True)


        return referencePrimaryMerge

    
    def orphan(self):

        # gather the all foreign key references
        refPrime = self.getRefPrime()

        # calculate the number of unique values in each primary key/reference table pair
        primary = refPrime.copy()
        primary = primary[["TabNam_primary", "ColNam"]].drop_duplicates()
        primary.columns = ["TabNam", "ColNam"]
        primary["UNIQUE_FREQ"] = primary.apply(self.calcUniqueFreq, axis=1)
        primary["Index"] = "Reference"

        # calculate the number of unique foreign keys present in each table that are present in the reference table
        countIn = refPrime.copy()
        countIn["UNIQUE_FREQ"] = countIn.apply(self.countIn, axis=1)
        countIn.columns = ["ColNam", "TabNam_prim", "TabNam", "UNIQUE_FREQ"]
        countIn["Index"] = "Count_In"
        countIn = countIn[["TabNam", "ColNam", "UNIQUE_FREQ", "Index"]]

        # calculate the number of unique foreign keys present in each table that are not present in the reference table
        countOut = refPrime.copy()
        countOut["UNIQUE_FREQ"] = countOut.apply(self.countOut, axis=1)
        countOut.columns = ["ColNam", "TabNam_prim", "TabNam", "UNIQUE_FREQ"]
        countOut["Index"] = "Count_Out"
        countOut = countOut[["TabNam", "ColNam", "UNIQUE_FREQ", "Index"]]

        # join the three tables and write to the orphan report
        orphanOutput = primary.append([countIn, countOut])

        # output the orphan test report
        self.query.outputReport(orphanOutput, "orphan.csv")


    def countOut(self, row):
        primary  =  row["TabNam_primary"]
        external =  row["TabNam_external"]
        col      =  row["ColNam"]
        query = f"""
            SELECT COUNT(DISTINCT({col})) as count
            FROM {self.query.prefix}{external} as ext
            WHERE NOT EXISTS 
            (
                SELECT {col}
                FROM {self.query.prefix}{primary} as prim
                WHERE prim.{col} = ext.{col}
            );"""

        CountOutResults = int(pandas.read_sql(query, con=self.query.conn)["count"])

        return CountOutResults

    def countIn(self, row):
        primary  =  row["TabNam_primary"]
        external =  row["TabNam_external"]
        col      =  row["ColNam"]

        query = f"""
            SELECT COUNT(DISTINCT(ext.{col})) as count
            FROM {self.query.prefix}{primary} prim LEFT JOIN {self.query.prefix}{external} ext ON prim.{col}=ext.{col}
        """

        CountInResults = int(pandas.read_sql(query, con=self.query.conn)["count"])

        return CountInResults


    def calcUniqueFreq(self, row):
        table   = row["TabNam"]
        col     = row["ColNam"]

        query = f"""
            SELECT COUNT(DISTINCT({col})) as count
            FROM {self.query.prefix}{table}
        """

        output = int(pandas.read_sql(query, con=self.query.conn)["count"])
        return output
    