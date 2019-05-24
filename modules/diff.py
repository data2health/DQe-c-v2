"""
Takes DQTBL and processes the dataset to reflect the current state of the database

input: DQTBL -> will look like tablelist.csv
output: tablelist.csv
"""

import math
import pandas

class Diff:
    def __init__(self, query: object):
        self.query = query

    
    def createDifference(self):

        ## gather all tables currently present and their sizes and row number
        DB_TBLs = self.dbSize()

        # merge the expected tables (DQTBL) and the actual tables (DB_TBLs)
        DQTBL = self.query.DQTBL.merge(DB_TBLs, on=["TabNam", "ColNam"], how="left")

        # mark all expected tables as either present or absent in the actual database
        DQTBL["loaded"] = DQTBL["Rows"].apply(lambda x: not math.isnan(x))


        ## ======================================================================================
        # CREATE TABLELIST REPORT
        # output the full report of the tablelist. This will be used to visualize the presence or
        # absence of tables and rows.

        tablelist = DQTBL[["TabNam", "ColNam", "Rows", "TotalSizeKB", "loaded", "primary"]].drop_duplicates()
        self.query.outputReport(tablelist, "tablelist.csv")


        ## If none of the tables appear to be loaded, the program quits.
        if len(tablelist[tablelist["loaded"]]) == 0:
            print ("This database seems to be empty. See the tablelist.csv report for more information.")
            exit()

        #tablelist.to_csv("reports/tablelist.csv")

        ## ======================================================================================

        # removes all table and col references that are not loaded or are empty in the actual database
        # this is mainly so we don't try and query non-existant tables down the road
        # write the DQTBL object to query to track our progress
        
        self.query.DQTBL = DQTBL[(DQTBL["loaded"]) | (DQTBL["Rows"] == 0)]


    def dbSize(self):
        if self.query.DBMS == "sql server":
            query = f"""
                SELECT 
                        t.NAME AS TabNam,
                        cols.COLUMN_NAME AS ColNam,
                        p.rows AS Rows,
                        SUM(a.total_pages) * 8 AS TotalSizeKB
                FROM sys.tables t
                INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
                INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
                INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN information_schema.columns cols ON t.NAME = cols.TABLE_NAME
                WHERE
                    t.NAME NOT LIKE 'dt%' AND t.is_ms_shipped = 0 AND i.OBJECT_ID > 255 AND
                    s.NAME = '{self.query.schema}' AND
                    cols.TABLE_SCHEMA = '{self.query.schema}'
                GROUP BY 
                    t.Name,
                    p.Rows,
                    cols.COLUMN_NAME
                ORDER BY t.Name"""


        elif self.query.DBMS == "oracle":
            query = """
                SELECT
                    TabNam,
                    TotalSizeKB,
                    NUM_ROWS AS Rows
                FROM
                    ((
                        SELECT
                            SEGMENT_NAME TabNam,
                            bytes/1000 TotalSizeKB
                        FROM user_segments
                        WHERE segment_name IN
                            (SELECT table_name
                                FROM all_tables)) d
                        INNER JOIN
                        (SELECT
                                TABLE_NAME,
                                NUM_ROWS
                            FROM all_tables) t
                        ON d.TabNam =t.TABLE_NAME
                    )"""


        elif self.query.DBMS == "redshift":
            query = f"""
                SELECT
                    info.table AS TabNam,
                    cols.column_name AS ColNam,
                    info.tbl_rows AS Rows, 
                    info.size * 1000 AS TotalSizeKB
                FROM SVV_TABLE_INFO info, information_schema.columns cols
                WHERE 
                    info.schema='{self.query.schema}' AND
                    cols.table_name = info.table; """
                
                
        elif self.query.DBMS == "postgresql":
            query = f"""
                SELECT
                    tabs.table_name as TabNam,
                    cols.column_name as colnam,
                    pg.reltuples::BIGINT as Rows,
                    pg_relation_size({self.query.query_prefix} tabs.table_name)/1000 as TotalSizeKB
                FROM pg_catalog.pg_class pg, information_schema.tables tabs, information_schema.columns cols
                WHERE 
                    pg.oid = to_regclass({self.query.query_prefix} tabs.table_name) AND
                    tabs.table_schema='{self.query.schema}' AND
                    tabs.table_catalog='{self.query.database}' AND
                    cols.table_schema='{self.query.schema}' AND
                    cols.table_catalog='{self.query.database}' AND
                    cols.table_name = tabs.table_name
                    ;"""

        output = pandas.read_sql(query, con=self.query.conn)
        output.columns = ["TabNam", "ColNam", "Rows", "TotalSizeKB"]

        return output
        
        