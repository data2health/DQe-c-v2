"""
Takes DQTBL and processes the dataset to reflect the current state of the database

input: DQTBL -> will look like tablelist.csv
output: tablelist.csv
"""

import math

class Diff:
    def __init__(self, query: object):
        self.query = query
        self.DQTBL = query.DQTBL

    
    def getDQTBL(self):

        ## gather all tables present and their sizes and row number
        DB_TBLs = self.query.dbSize()

        # merge the expected tables (DQTBL) and the actual tables (DB_TBLs)
        DQTBL = self.DQTBL.merge(DB_TBLs, on=["TabNam", "ColNam"], how="left")

        # mark all expected tables as either present or absent in the actual database
        DQTBL["loaded"] = DQTBL["Rows"].apply(lambda x: not math.isnan(x))


        ## ======================================================================================
        # CREATE TABLELIST REPORT
        # output the full report of the tablelist. This will be used to visualize the presence or
        # absence of tables and rows.

        tablelist = DQTBL[["TabNam", "ColNam", "Rows", "TotalSizeKB", "loaded"]].drop_duplicates()
        tablelist.to_csv("reports/tablelist.csv")

        ## ======================================================================================

        # remove all table and col references that are not loaded in the actual database
        # this is mainly so we don't try and query non-existant tables down the road
        DQTBL = DQTBL[DQTBL["loaded"]]
        return DQTBL
        
        