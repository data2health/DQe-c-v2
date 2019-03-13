"""
Will hold all SQL queries and other modules will call specified methods from it.
"""

import pandas

class Query:
    def __init__(self, prep: object):
        self.DBMS = prep.DBMS
        self.schema = prep.schema
        self.database = prep.database
        self.conn = prep.conn
        if prep.schema != "":
            self.prefix = prep.database + "." + prep.schema + "."
            self.query_prefix = f"'{prep.schema}.' ||"
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
    
    def dbSize(self):
        if self.DBMS == "sql server":
            query = """SELECT 
                        t.NAME AS TabNam,
                        p.rows AS Rows,
                        SUM(a.total_pages) * 8 AS TotalSizeKB
                    FROM sys.tables t
                    INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
                    INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
                    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                    LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE t.NAME NOT LIKE 'dt%' AND t.is_ms_shipped = 0 AND i.OBJECT_ID > 255 
                    GROUP BY 
                        t.Name,
                        p.Rows
                    ORDER BY t.Name"""


        elif self.DBMS == "oracle":
            query = """
            SELECT
                TabNam,
                TotalSizeKB,
                NUM_ROWS AS Rows
            FROM
                (
                    (
                    SELECT
                        SEGMENT_NAME TabNam,
                        bytes/1000 TotalSizeKB
                    FROM user_segments
                    WHERE segment_name IN
                        (
                            SELECT table_name
                            FROM all_tables
                        )
                    ) d
                    INNER JOIN
                    (
                        SELECT
                            TABLE_NAME,
                            NUM_ROWS
                        FROM all_tables
                    ) t
                    ON d.TabNam =t.TABLE_NAME
                )"""


        elif self.DBMS == "redshift":
            query = f"""
                SELECT
                    info.table AS TabNam, 
                    info.tbl_rows AS Rows, 
                    info.size * 1000 AS TotalSizeKB
                FROM SVV_TABLE_INFO info
                WHERE info.schema='{self.schema}' ;"""
                
                
        elif self.DBMS == "postgresql":
            query = f"""
                SELECT
                    tabs.table_name as TabNam,
                    pg.reltuples::BIGINT as Rows,
                    pg_relation_size({self.query_prefix} tabs.table_name)/1000 as TotalSizeKB
                FROM pg_catalog.pg_class pg, information_schema.tables tabs
                WHERE 
                    pg.oid = to_regclass({self.query_prefix} tabs.table_name) AND
                    tabs.table_schema='{self.schema}' AND
                    tabs.table_catalog='{self.database}';"""
        output = pandas.read_sql(query, con=self.conn)
        output.columns = ["TabNam", "Rows", "TotalSizeKB"]

        return output