"""
Takes DQTBL and processes the dataset to reflect the current state of the database

input: DQTBL
output: tablelist.csv
"""


class Diff:
    def __init__(self, prep: object, query: object):
