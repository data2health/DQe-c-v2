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
    def __init__(self, CDM: str):
        # ==================================
        # TODO: change to take the already 
        # created DQTBL from prep.py instead of CDM
        # ==================================
        with open('config.json') as json_file:
            self.config = json.load(json_file)
        
        self.DQTBL: object = {
                                "PCORNET3": pandas.read_csv("./CDMs/DQTBL_pcornet_v3.csv"),
                                "PCORNET31": pandas.read_csv("./CDMs/DQTBL_pcornet_v31.csv"),
                                "OMOPV5_0": pandas.read_csv("./CDMs/DQTBL_omop_v5_0.csv"),
                                "OMOPV5_2": pandas.read_csv("./CDMs/DQTBL_omop_v5_2.csv"),
                                "OMOPV5_3": pandas.read_csv("./CDMs/DQTBL_omop_v5_3.csv"),
                             }[CDM]


    def getRefPrim(self):

        ## gather primary keys from DQTBL
        primaryPairs: object = self.DQTBL[(self.DQTBL["primary"]) & (self.DQTBL["cat"].isin(["clinical", "health_system"]))][["ColNam", "TabNam"]]
        

        ## gather all non-primary table-column pairs
        referencePairs: object = self.DQTBL[(self.DQTBL["primary"] == False) & (self.DQTBL["cat"].isin(["clinical", "health_system"]))][["ColNam", "TabNam"]]
        

        ## merge primary and reference tables and remove all non-reference keys
        referencePrimaryMerge: object = primaryPairs.merge(referencePairs, on="ColNam", how="right", suffixes=("_primary", "_reference"))
        referencePrimaryMerge.dropna(subset = ["TabNam_primary"], inplace=True)

        
        return referencePrimaryMerge

    
    def orphanCalc(self):
        q = Query()
        refPrim = self.getRefPrim()
