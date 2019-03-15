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
    def __init__(self, prep: object, query: object):
        # ==================================
        # Initiates the orphan class
        # creates the DQTBL object
        # ==================================

        self.DQTBL = prep.DQTBL
        self.prep = prep
        self.query = query


    def getRefPrime(self):

        ## gather primary keys from DQTBL
        primaryPairs: object = self.DQTBL[(self.DQTBL["primary"]) & (self.DQTBL["cat"].isin(["clinical", "health_system"]))][["ColNam", "TabNam"]]
        

        ## gather all non-primary table-column pairs
        referencePairs: object = self.DQTBL[(self.DQTBL["primary"] == False) & (self.DQTBL["cat"].isin(["clinical", "health_system"]))][["ColNam", "TabNam"]]
        

        ## merge primary and reference tables and remove all non-reference keys
        referencePrimaryMerge: object = primaryPairs.merge(referencePairs, on="ColNam", how="right", suffixes=("_primary", "_reference"))
        referencePrimaryMerge.dropna(subset = ["TabNam_primary"], inplace=True)

        
        return referencePrimaryMerge

    
    def orphanCalc(self):
        # q = Query(self.prep)
        q = self.query
        refPrime = self.getRefPrime()

        #refPrime["CountOut"] = refPrime.apply(lambda row: q.Orphan(row["TabNam_primary"], row["TabNam_reference"], row["ColNam"])["count"], axis=1)

        print (refPrime)
