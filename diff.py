"""
Takes DQTBL and processes the dataset to reflect the current state of the database

input: DQTBL -> will look like tablelist.csv
output: tablelist.csv
"""


class Diff:
    def __init__(self, prep: object, query: object):
        self.DQTBL = prep.DQTBL
        self.query = query

    
    def DQTBL_diff(self):
        DB_TBLs = self.query.