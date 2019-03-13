"""
Will hold all SQL queries and other modules will call specified methods from it.
"""

import pandas

class Query:
    def __init__(self, prep):
        self.DBMS = prep.DBMS
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
    
    def dbSize(self):
        if self.DBMS == "sql server":
        SELECT 
            t.NAME AS Repo_Tables,
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
        ORDER BY t.Name


-- Database: "Oracle"
SELECT
    Repo_Tables, TotalSizeKB,
    NUM_ROWS
FROM
    (
        (
        SELECT
            SEGMENT_NAME Repo_Tables,
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
        ON d.Repo_Tables =t.TABLE_NAME
    )


-- Database: "Redshift"
SELECT
    info.table AS Repo_Tables, 
    info.tbl_rows AS Rows, 
    info.size * 1000 AS TotalSizeKB
FROM SVV_TABLE_INFO info
WHERE info.schema='", schema_orig, "' ;
  
  
--   Database: "PostgreSQL"
SELECT
    tabs.table_name, 
    pg.reltuples::BIGINT, 
    pg_relation_size(tabs.table_name)/1000
FROM pg_catalog.pg_class pg, information_schema.tables tabs
WHERE tabs.table_name=pg.relname AND tabs.table_schema='public' ;