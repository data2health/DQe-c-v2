"""
Will hold all SQL queries and other modules will call specified methods from it.
"""

import pandas

class Query:
    def __init__(self, prep):
        self.conn = prep.conn
        if prep.schema != "":
            self.prefix = prep.database + "." + prep.schema + "."
        else:
            self.prefix = ""

    def Missingness(self, table, col):
        pass

    def Indicator(self):
        pass
    
    def Orphan(self, primary, external, col):
        
        query = f"""
            SELECT COUNT(DISTINCT({col}))
            FROM {self.prefix}{external}
            WHERE {col} NOT IN(
                SELECT DISTINCT({col})
                FROM {self.prefix}{primary});"""
        
        return pandas.read_sql(query, con=self.conn)