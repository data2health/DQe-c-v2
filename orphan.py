"""
computes the orphan keys in a few main tables


expected input: orphan_keys.csv

|   Table    |   Primary Key    |
|------------|------------------|
|   person   |    person_id     |

expected output: orphan.csv
"""
import pandas
import json
from query import Query

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

    
    def orphanCalc(self):

        # gather the all foreign key references
        refPrime = self.getRefPrime()[["TabNam_primary", "ColNam", "TabNam_external"]]

        # calcute the number of foreign keys that are not present in their reference table
        refPrime["CountOut"] = refPrime.apply(lambda x: self.orphan(x), axis=1)
        
        # combine results with the main DQTBL object
        append_merge = self.DQTBL.merge(refPrime, left_on=["TabNam", "ColNam"], right_on=["TabNam_external", "ColNam"], how="left")
        append_merge.rename(index=str, columns={"TabNam_primary": "reference"}, inplace=True)
        
        # calculate the number of foreign keys that are present in their reference table
        append_merge["CountIn"] = append_merge.apply(lambda x: x["UNIQUE_FREQ"]-x["CountOut"], axis=1)

        # reorganize dataframe and remove redundant columns
        output_DQTBL = append_merge[['TabNam', 'ColNam', 'primary', 'DQLVL', 'abbr', 'cat',
                                    'Rows', 'TotalSizeKB', 'loaded', 'UNIQUE_FREQ', 'CountIn', 
                                    'CountOut', 'reference', 'MS1_FREQ', 'MS2_FREQ', 
                                    'MSs_PERCENTAGE', 'ORGANIZATION', 'CDM', 'TEST_DATE']]

        self.query.DQTBL = output_DQTBL


    def orphan(self, row):
        primary  =  row["TabNam_primary"]
        external =  row["TabNam_external"]
        col      =  row["ColNam"]
        query = f"""
            SELECT COUNT(DISTINCT({col}))
            FROM {self.query.prefix}{external} as ext
            WHERE NOT EXISTS 
            (
                SELECT {col}
                FROM {self.query.prefix}{primary} as prim
                WHERE prim.{col} = ext.{col}
            );"""

        CountOutResults = int(pandas.read_sql(query, con=self.query.conn)["count"])

        return CountOutResults
    