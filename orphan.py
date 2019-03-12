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

class Orphan:
    def __init__(self, CDM: str):
        with open('configs.json') as json_file:
            self.config = json.load(json_file)
        
        self.DQTBL: object = {
                                "PCORNET3": pandas.read_csv("./CDMs/DQTBL_pcornet_v3.csv"),
                                "PCORNET31": pandas.read_csv("./CDMs/DQTBL_pcornet_v31.csv"),
                                "OMOPV5_0": pandas.read_csv("./CDMs/DQTBL_omop_v5_0.csv"),
                                "OMOPV5_2": pandas.read_csv("./CDMs/DQTBL_omop_v5_2.csv"),
                                "OMOPV5_3": pandas.read_csv("./CDMs/DQTBL_omop_v5_3.csv"),
                             }[CDM]


    def primary(self):

        ## gather primary keys from DQTBL
        primaryPairs: object = self.DQTBL[(self.DQTBL["primary"]) & (self.DQTBL["cat"].isin(["clinical", "health_system"]))][["ColNam", "TabNam"]]
        #print (primaryPairs)

        ## gather all non-primary table-column pairs
        referencePairs: object = self.DQTBL[(self.DQTBL["primary"] == False) & (self.DQTBL["cat"].isin(["clinical", "health_system"]))][["ColNam", "TabNam"]]
        #print (referencePairs)

        ## combine the two tables
        mergedPairs = primaryPairs.merge(referencePairs, on="ColNam", how="right", suffixes=("_primary", "_reference"))

        mergedPairs.dropna(subset = ["TabNam_primary"], inplace=True)
        
        return mergedPairs