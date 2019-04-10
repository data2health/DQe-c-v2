"""
Will hold all SQL queries and other modules will call specified methods from it.
"""

import pandas
import datetime
from prep import Prep

class Query:
    def __init__(self):
        prep = Prep()
        self.DQTBL = prep.DQTBL
        self.CDM = prep.CDM
        self.DBMS = prep.DBMS
        self.organization = prep.organization
        
        self.database = prep.database
        self.conn = prep.conn

        self.schema = prep.schema
        if self.schema != "":
            self.prefix = prep.database + "." + prep.schema + "."
            self.query_prefix = f"'{prep.schema}.' ||"
        else:
            self.prefix = ""
            self.query_prefix = ""

    ## Calculates missingness for each row (i.e. each table) and adds to DQTBL which is later printed in missingness.csv
    def missingnessCalc(self, row):
        nonsense = "'+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI'"
    
        freqQuery = f"""
                SELECT COUNT(*)
                FROM {self.prefix}{row.TabNam} ;"""

        uniqFreqQuery = f"""
                        SELECT COUNT(DISTINCT {row.ColNam})
                        FROM {self.prefix}{row.TabNam} ;"""
        
        if (self.DBMS == "sql server" or self.DBMS == "redshift"):
            ms1FreqQuery = f"""
                        SELECT COUNT('{row.ColNam}')
                        FROM {self.prefix}{row.TabNam}
                        WHERE ['{row.ColNam}'] IS NULL OR CAST('{row.ColNam}' AS VARCHAR) IN ('') ;"""

            ms2FreqQuery = f"""
                        SELECT COUNT('{row.ColNam}')
                        FROM {self.prefix}{row.TabNam}
                        WHERE CAST('{row.ColNam}' AS VARCHAR) IN ({nonsense}) ;"""
            
        elif self.DBMS == "oracle":
            ms1FreqQuery = f"""
                        SELECT COUNT('{row.ColNam}')
                        FROM {self.prefix}{row.TabNam}
                        WHERE {row.ColNam} IS NULL OR TO_CHAR({row.ColNam}) IN ('') ;"""

            ms2FreqQuery = f"""
                        SELECT COUNT('{row.ColNam}')
                        FROM {self.prefix}{row.TabNam}
                        WHERE TO_CHAR({row.ColNam}) IN ({nonsense}) ;"""

        elif self.DBMS == "postgresql":
            ms1FreqQuery = f"""
                        SELECT COUNT('{row.ColNam}')
                        FROM {self.prefix}{row.TabNam} 
                        WHERE '{row.ColNam}' IS NULL OR CAST('{row.ColNam}' AS VARCHAR) IN ('') ;"""
                        
            ms2FreqQuery = f"""
                        SELECT COUNT('{row.ColNam}')
                        FROM {self.prefix}{row.TabNam}
                        WHERE CAST('{row.ColNam}' AS VARCHAR) IN ({nonsense}) ;"""
        
        """
        Incorporate new missingness test for other database management systems here
        add:
        elif self.DBMS == 'new db':
            ms1FreqQuery = query

            ms2FreqQuery = query
        """

        row["TEST_DATE"] = datetime.datetime.today().strftime('%m-%d-%Y')
        #row["FREQ"] = pandas.read_sql(freqQuery, con=self.conn).values[0][0]
        row["UNIQUE_FREQ"] = pandas.read_sql(uniqFreqQuery, con=self.conn).values[0][0]
        row["MS1_FREQ"] = pandas.read_sql(ms1FreqQuery, con=self.conn).values[0][0]
        row["MS2_FREQ"] = pandas.read_sql(ms2FreqQuery, con=self.conn).values[0][0]

        try:
            row["MSs_PERCENTAGE"] = round((row["MS1_FREQ"]+row["MS2_FREQ"])/row["Rows"], 3)
        except ZeroDivisionError:
            row["MSs_PERCENTAGE"] = round(0, 3)
            
        row["ORGANIZATION"] = self.organization
        row["CDM"] = self.CDM

        return row

    def Indicator(self):
        pass
    
    def Orphan(self, row):
        primary = row["TabNam_primary"]
        external = row["TabNam_external"]
        col = row["ColNam"]
        
        ## counts how many foreign keys are not found in their primary table
        CountOut = f"""
            SELECT COUNT(DISTINCT({col}))
            FROM {self.prefix}{external} as ext
            WHERE NOT EXISTS 
            (
                SELECT {col}
                FROM {self.prefix}{primary} as prim
                WHERE prim.{col} = ext.{col}
            );"""

        CountOutResults = int(pandas.read_sql(CountOut, con=self.conn)["count"])

        return CountOutResults
    
    
    def dbSize(self):
        if self.DBMS == "sql server":
            query = """
                SELECT 
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
                    cols.column_name as colnam,
                    pg.reltuples::BIGINT as Rows,
                    pg_relation_size({self.query_prefix} tabs.table_name)/1000 as TotalSizeKB
                FROM pg_catalog.pg_class pg, information_schema.tables tabs, information_schema.columns cols
                WHERE 
                    pg.oid = to_regclass({self.query_prefix} tabs.table_name) AND
                    tabs.table_schema='{self.schema}' AND
                    tabs.table_catalog='{self.database}' AND
                    cols.table_schema='{self.schema}' AND
                    cols.table_catalog='{self.database}' AND
                    cols.table_name = tabs.table_name
                    ;"""

        output = pandas.read_sql(query, con=self.conn)
        output.columns = ["TabNam", "ColNam", "Rows", "TotalSizeKB"]
        return output