"""
Will hold all SQL queries and other modules will call specified methods from it.
"""

class Query:
    def __init__(self, connection):
        self.conn = connection

    def Missingness(self):
        pass

    def Indicator(self):
        pass
    
    def Orphan(self):
        pass