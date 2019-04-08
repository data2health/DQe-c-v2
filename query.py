import pandas
import datetime
from typing import Dict, List

class Query:
    def __init__(self, prep: object):
        self.CDM = prep.CDM
        self.DBMS = prep.DBMS
        self.organization = prep.organization
        self.schema = prep.schema
        self.database = prep.database
        self.conn = prep.conn
        if prep.schema != "":
            self.prefix = prep.database + "." + prep.schema + "."
            self.query_prefix = f"'{prep.schema}.' ||"
        else:
            self.prefix = ""

    def missingnessCalc(self, row, func):
        freqQuery = f"""
                SELECT COUNT(*)
                FROM {self.schema}{row.TabNam} ;"""

        uniqFreqQuery = f"""
                        SELECT COUNT(DISTINCT {row.ColNam})
                        FROM {self.schema}{row.TabNam} ;"""

        ms1FreqQuery, ms2FreqQuery = func(row)

        row["TEST_DATE"] = datetime.datetime.today().strftime('%m-%d-%Y')
        row["FREQ"] = pandas.read_sql(freqQuery, con=self.conn).values[0][0]
        row["UNIQUE_FREQ"] = pandas.read_sql(uniqFreqQuery, con=self.conn).values[0][0]
        row["MS1_FREQ"] = pandas.read_sql(ms1FreqQuery, con=self.conn).values[0][0]
        row["MS2_FREQ"] = pandas.read_sql(ms2FreqQuery, con=self.conn).values[0][0]
        row["MSs_PERCENTAGE"] = round((row["MS1_FREQ"]+row["MS2_FREQ"])/row["FREQ"], 3)
        row["ORGANIZATION"] = self.organization
        row["CDM"] = self.CDM

        return row
    
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

    def withoutdemPCORI(self, col: str, group: str) -> object:
        exclude: Dict[str,List[str]] = {
                "Gender": ["M","F"],
                "Race": ["05","03","07","02","01","04","06","OT"],
                "Ethnicity": ["Y"]
            }
        
        if self.DBMS == "sql server" or self.DBMS == "oracle":
            denominatorQuery: str = f"""
                                    SELECT COUNT(DISTINCT(PATID))
                                    FROM DEMOGRAPHIC
                                    WHERE BIRTH_DATE > 1900-01-01 AND BIRTH_DATE  < 2014-01-01 """
            notinQuery: str = f"""
                            SELECT COUNT(PATID)
                            FROM  (
                                    SELECT *
                                    FROM {self.schema}{self.prefix} DEMOGRAPHIC
                                    WHERE BIRTH_DATE > 1900-01-01 AND BIRTH_DATE  < 2014-01-01
                                    ) dd
                            WHERE toupper({col}) NOT IN {exclude[group]} """
            whattheyhaveQuery: str = f"""
                                    SELECT DISTINCT(toupper({col}))
                                    FROM   (
                                            SELECT *
                                            FROM {self.schema}{self.prefix} DEMOGRAPHIC
                                            WHERE BIRTH_DATE > 1900-01-01 AND BIRTH_DATE  < 2014-01-01
                                            ) dd
                                    WHERE toupper({col}) NOT IN {exclude[group]} """
        
        cursor = self.conn.cursor()
        denominator = cursor.execute(denominatorQuery)
        notin = cursor.execute(notinQuery)
        whattheyhave = cursor.execute(whattheyhaveQuery)
        self.conn.close()

        d1: int = round((notin/denominator)*100,4)
        # MESSAGES
        # print("%s of patients born between 1900-01-01 and 2014-01-01 are missing %s information.", d1, list)
        # if (d1 > 0):
        #     print("%s of the %s patients born between 1900-01-01 and 2014-01-01 don't have an acceptable %s record in the %s table.", notin, denominator, toupper(list), toupper(df))
        #     print("Unacceptable values in column %s are %s.", toupper(col), whattheyhave)
        return pandas.DataFrame({
                                "GROUP": [group],
                                "MISSING PERCENTAGE": [d1],
                                "MISSING POPULATION": [notin],
                                "DENOMINATOR": [denominator],
                                "PERCENTAGE": [str(round(d1,2))+"%"],
                                "TEST_DATE": [datetime.datetime.today().strftime('%m-%d-%Y')],
                                "ORGANIZATION": [self.organization],
                                "CDM": [self.CDM]
                                })

    def withoutPCORI(self, table: str, col: str, group: str) -> object:
        exclude: List[str] = ["","%","$","#","@","NI"]
        denominatorQuery: str = f"""
                                SELECT COUNT(DISTINCT(PATID))
                                FROM {self.schema}{self.prefix}DEMOGRAPHIC
                                WHERE BIRTH_DATE > 1900-01-01 AND BIRTH_DATE  < 2014-01-01 """


        if self.DBMS == "sql server":
            pats_wit_oneoutQuery: str = f"""
                                        SELECT COUNT(DISTINCT(PATID))
                                        FROM {self.schema}{table}
                                        WHERE toupper({col}) IS NULL OR CAST(toupper({col}) AS CHAR(54)) IN  {exclude} """
            patsWithoutRecordsQuery: str = f"""
                                            SELECT COUNT(DISTINCT(PATID))
                                            FROM {self.schema}{self.prefix}DEMOGRAPHIC
                                            WHERE BIRTH_DATE > 1900-01-01
                                                AND BIRTH_DATE  < 2014-01-01
                                                AND PATID IN  (
                                                            SELECT DISTINCT(PATID)
                                                            FROM {self.schema}{table}
                                                            WHERE toupper({col}) IS NOT NULL AND CAST(toupper({col}) AS CHAR(54)) NOT IN {exclude}
                                                            ) """

        elif self.DBMS == "oracle":
            pats_wit_oneoutQuery: str = f"""
                                        SELECT COUNT(DISTINCT(PATID))
                                        FROM {self.schema}{table}
                                        WHERE toupper({col}) IS NULL OR TO_CHAR(toupper({col})) IN {exclude} """
            
            patsWithoutRecordsQuery: str = f"""
                                            SELECT COUNT(DISTINCT(PATID))
                                            FROM {self.schema}{self.prefix}DEMOGRAPHIC
                                            WHERE BIRTH_DATE > 1900-01-01
                                                AND BIRTH_DATE  < 2014-01-01
                                                AND PATID IN  (
                                                            SELECT DISTINCT(PATID)
                                                            FROM {self.schema}{table}
                                                            WHERE toupper({col}) IS NOT NULL AND TO_CHAR(toupper({col})) NOT IN {exclude}
                                                            ) """

        cursor = self.conn.cursor()
        denominator = cursor.execute(denominatorQuery)
        pats_wit_oneout = cursor.execute(pats_wit_oneoutQuery)
        patsWithoutRecords = cursor.execute(patsWithoutRecordsQuery)
        self.conn.close()

        ppwo: int = round((pats_wit_oneout/denominator)*100,4)
        pwse: int = round(((denominator-patsWithoutRecords)/denominator)*100,4)
        # if (ppwo > 1) message(pats_wit_oneout, " of the patients -- ",ppwo,"% of patients -- are missing at least 1 acceptable ",toupper(col)," value in the ",toupper(table)," table.",appendLF=T)
        # if (pwse > 1) message(patsWithoutRecords, " of the patients -- ",pwse,"% of patients -- are missing any acceptable ",toupper(col)," value in the ",toupper(table)," table.",appendLF=T)
        # message(pwse, "% of unique patients don't have any '", list.name,"' record in the ",df.name, " table.",appendLF=T)
        return pandas.DataFrame({
                                "GROUP": [group],
                                "MISSING PERCENTAGE": [pwse],
                                "MISSING POPULATION": [patsWithoutRecords],
                                "DENOMINATOR": [denominator],
                                "PERCENTAGE": [str(round(pwse,2))+"%"],
                                "TEST_DATE": [datetime.datetime.today().strftime('%m-%d-%Y')],
                                "ORGANIZATION": [self.organization],
                                "CDM": [self.CDM]
                                })
    
    def withoutdemOMOP(self, col: str, group: str) -> object:
        genderQuery: str = f"""
                            SELECT concept_id
                            FROM {self.schema}{self.prefix} CONCEPT
                            WHERE domain_id = "Gender" """

        raceQuery: str = f"""
                        SELECT concept_id
                        FROM {self.schema}{self.prefix} CONCEPT
                        WHERE domain_id= "Race" """

        cursor = self.conn.cursor()

        exclude: Dict[str, List] = {
                "Gender": [cursor.execute(genderQuery)], # unsure if brackets are needed. check when testing
                "Race": [cursor.execute(raceQuery)], # unsure if brackets are needed. check when testing
                "Ethnicity": [38003563,38003564]
            }

        if self.DBMS == "oracle":
            denominatorQuery: str = f"""
                                    SELECT COUNT(DISTINCT(PATID))
                                    FROM DEMOGRAPHIC
                                    WHERE BIRTH_DATE > "1900-01-01" AND BIRTH_DATE  < "2018-10-01" """
            notinQuery: str = f"""
                            SELECT COUNT(PATID)
                            FROM (SELECT *
                                    FROM {self.schema}{self.prefix} DEMOGRAPHIC
                                    WHERE BIRTH_DATE > "1900-01-01" AND BIRTH_DATE  < "2018-10-01" )
                            WHERE toupper({col}) NOT IN {exclude[group]} """
            
            whattheyhaveQuery: str = f"""
                                    SELECT DISTINCT(toupper({col}))
                                    FROM (SELECT *
                                        FROM {self.schema}{self.prefix} DEMOGRAPHIC
                                        WHERE BIRTH_DATE > "1900-01-01" AND BIRTH_DATE  < "2018-10-01" )
                                    WHERE toupper({col}) NOT IN {exclude[group]} """

        elif self.DBMS == "sql server" or self.DBMS == "redshift":
            denominatorQuery: str = f"""
                                    SELECT COUNT(DISTINCT(person_id))
                                    FROM {self.schema}{self.prefix} PERSON
                                    WHERE CONVERT(DATETIME,
                                                CAST([year_of_birth] AS VARCHAR(4))+'-'+
                                                CAST([month_of_birth] AS VARCHAR(2))+'-'+
                                                CAST([day_of_birth] AS VARCHAR(2))) > "1900-01-01" 
                                        AND CONVERT(DATETIME,
                                                    CAST([year_of_birth] AS VARCHAR(4))+'-'+
                                                    CAST([month_of_birth] AS VARCHAR(2))+'-'+
                                                    CAST([day_of_birth] AS VARCHAR(2)))  < "2018-10-01" """
            notinQuery: str = f"""
                            SELECT COUNT(person_id) 
                            FROM (SELECT * 
                                    FROM {self.schema}{self.prefix} PERSON
                                    WHERE CONVERT(DATETIME,
                                                CAST([year_of_birth] AS VARCHAR(4))+'-'+
                                                CAST([month_of_birth] AS VARCHAR(2))+'-'+
                                                CAST([day_of_birth] AS VARCHAR(2))) > "1900-01-01" 
                                        AND CONVERT(DATETIME,
                                                    CAST([year_of_birth] AS VARCHAR(4))+'-'+
                                                    CAST([month_of_birth] AS VARCHAR(2))+'-'+
                                                    CAST([day_of_birth] AS VARCHAR(2)))  < "2018-10-01" ) dd
                            WHERE toupper({col}) NOT IN {exclude[group]} """
            whattheyhaveQuery: str = f"""
                                    SELECT DISTINCT(toupper({col})) 
                                    FROM (SELECT * 
                                        FROM {self.schema}{self.prefix} PERSON
                                        WHERE CONVERT(DATETIME,
                                                        CAST([year_of_birth] AS VARCHAR(4))+'-'+
                                                        CAST([month_of_birth] AS VARCHAR(2))+'-'+
                                                        CAST([day_of_birth] AS VARCHAR(2))) > "1900-01-01" 
                                                AND CONVERT(DATETIME,
                                                            CAST([year_of_birth] AS VARCHAR(4))+'-'+
                                                            CAST([month_of_birth] AS VARCHAR(2))+'-'+
                                                            CAST([day_of_birth] AS VARCHAR(2)))  < "2018-10-01") dd
                                    WHERE toupper({col}) NOT IN {exclude[group]} """
        elif self.DBMS == "postgresql":
            denominatorQuery: str = f"""
                                    SELECT COUNT(DISTINCT(person_id))
                                    FROM {self.schema}{self.prefix} PERSON
                                    WHERE CAST(concat(CAST(year_of_birth AS VARCHAR(4)),'-',
                                                    CAST(month_of_birth AS VARCHAR(2)),'-',
                                                    CAST(day_of_birth AS VARCHAR(2))) AS DATE) > "1900-01-01" 
                                        AND CAST(concat(CAST(year_of_birth AS VARCHAR(4)),'-',
                                                        CAST(month_of_birth AS VARCHAR(2)),'-',
                                                        CAST(day_of_birth AS VARCHAR(2))) AS DATE)  < "2018-10-01" """
            notinQuery: str = f"""
                            SELECT COUNT(person_id)
                            FROM (SELECT * 
                                    FROM {self.schema}{self.prefix} PERSON
                                    WHERE CAST(concat(CAST(year_of_birth AS VARCHAR(4)),'-',
                                                    CAST(month_of_birth AS VARCHAR(2)),'-',
                                                    CAST(day_of_birth AS VARCHAR(2))) AS DATE) > "1900-01-01" 
                                        AND CAST(concat(CAST(year_of_birth AS VARCHAR(4)),'-',
                                                        CAST(month_of_birth AS VARCHAR(2)),'-',
                                                        CAST(day_of_birth AS VARCHAR(2))) AS DATE)  < "2018-10-01") dd
                            WHERE toupper(col) NOT IN {exclude[group]} """
            whattheyhaveQuery: str = f"""
                                    SELECT DISTINCT(toupper(col)) 
                                    FROM (SELECT * 
                                        FROM {self.schema}{self.prefix} PERSON 
                                        WHERE CAST(concat(CAST(year_of_birth AS VARCHAR(4)),'-',
                                                            CAST(month_of_birth AS VARCHAR(2)),'-',
                                                            CAST(day_of_birth AS VARCHAR(2))) AS DATE) > "1900-01-01" 
                                                AND CAST(concat(CAST(year_of_birth AS VARCHAR(4)),'-',
                                                                CAST(month_of_birth AS VARCHAR(2)),'-',
                                                                CAST(day_of_birth AS VARCHAR(2))) AS DATE)  < "2018-10-01") dd
                                    WHERE toupper(col) NOT IN {exclude[group]} """

        denominator = cursor.execute(denominatorQuery)
        notin = cursor.execute(notinQuery)
        whattheyhave = cursor.execute(whattheyhaveQuery)
        self.conn.close()

        d1: int = round((notin/denominator)*100,4)

        # message(d1, "% of patients born between ",1900-01-01," and ",2018-10-01, " are missing ", group," information.",appendLF=T)
        # if (d1 > 0) message(notin, " of the ",denominator, " patients born between ",1900-01-01," and ",2018-10-01, " don't have an acceptable ", toupper(group), " record in the ",toupper(table), " table.",appendLF=T)
        # if (d1 > 0) message("Unacceptable values in column ", toupper(col), " are ",whattheyhave,".",appendLF=T)
        
        return pandas.DataFrame({
                                "GROUP": [group],
                                "MISSING PERCENTAGE": [d1],
                                "MISSING POPULATION": [notin],
                                "DENOMINATOR": [denominator],
                                "PERCENTAGE": [str(round(d1,2))+"%"],
                                "TEST_DATE": [datetime.datetime.today().strftime('%m-%d-%Y')],
                                "ORGANIZATION": [self.organization],
                                "CDM": [self.CDM]
                                })

    def withoutOMOP(self, table: str, col: str, group: str) -> object:
        exclude: List[str] = ["","%","$","#","@","NI"]

        if self.DBMS == "oracle":
            denominatorQuery: str = f"""
                                    SELECT COUNT(DISTINCT(PATID))
                                    FROM {self.schema}{self.prefix} DEMOGRAPHIC
                                    WHERE BIRTH_DATE > "1900-01-01" AND BIRTH_DATE  < "2018-10-01" """
            
            pats_wit_oneoutQuery: str = f"""
                                        SELECT COUNT(DISTINCT(PATID))
                                        ###CHECK HERE###
                                        FROM {self.schema}subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table))," 
                                        WHERE toupper({col}) IS NULL OR TO_CHAR(toupper({col})) IN {exclude} """

            patsWithoutRecordsQuery: str = f"""
                                        SELECT COUNT(DISTINCT(PATID))
                                        FROM {self.schema}{self.prefix} DEMOGRAPHIC
                                        WHERE BIRTH_DATE > "1900-01-01" AND BIRTH_DATE  < "2018-10-01"
                                            AND PATID IN (SELECT DISTINCT(PATID)
                                                            ###CHECK HERE###
                                                            FROM {self.schema}subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table)),"
                                                            WHERE toupper({col}) IS NOT NULL AND TO_CHAR(toupper({col})) NOT IN  {exclude}) """
        
        elif self.DBMS == "sql server" or self.DBMS == "redshift":
            denominatorQuery: str = f"""
                                    SELECT COUNT(DISTINCT(person_id))
                                    FROM {self.schema}{self.prefix} PERSON
                                    WHERE CONVERT(DATETIME,
                                                CAST([year_of_birth] AS VARCHAR(4))+'-'+
                                                CAST([month_of_birth] AS VARCHAR(2))+'-'+
                                                CAST([day_of_birth] AS VARCHAR(2))) > "1900-01-01" 
                                        AND CONVERT(DATETIME,
                                                    CAST([year_of_birth] AS VARCHAR(4))+'-'+
                                                    CAST([month_of_birth] AS VARCHAR(2))+'-'+
                                                    CAST([day_of_birth] AS VARCHAR(2)))  < "2018-10-01" """
            pats_wit_oneoutQuery: str = f"""
                                        SELECT COUNT(DISTINCT(person_id)) 
                                        ###CHECK HERE###
                                        FROM {self.schema}subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower({table}))," 
                                        WHERE toupper({col}) IS NULL OR CAST(toupper({col}) AS CHAR(54)) IN {exclude} """
            patsWithoutRecordsQuery: str = f"""
                                        SELECT COUNT(DISTINCT(person_id))
                                        FROM {self.schema}{self.prefix} PERSON
                                        WHERE CONVERT(DATETIME,
                                                        CAST([year_of_birth] AS VARCHAR(4))+'-'+
                                                        CAST([month_of_birth] AS VARCHAR(2))+'-'+
                                                        CAST([day_of_birth] AS VARCHAR(2))) > "1900-01-01" 
                                                AND CONVERT(DATETIME,
                                                            CAST([year_of_birth] AS VARCHAR(4))+'-'+
                                                            CAST([month_of_birth] AS VARCHAR(2))+'-'+
                                                            CAST([day_of_birth] AS VARCHAR(2)))  < "2018-10-01"
                                                AND person_id IN (SELECT DISTINCT(person_id)
                                                                ###CHECK HERE### 
                                                                FROM {self.schema},subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower({table}))," 
                                                                WHERE toupper({col}) IS NOT NULL AND CAST(toupper({col}) AS CHAR(54)) NOT IN {exclude}) """

        elif self.DBMS == "postgresql":
            denominatorQuery: str = f"""
                                    SELECT COUNT(DISTINCT(person_id))
                                    FROM {self.schema}{self.prefix} PERSON
                                    WHERE CAST(concat(CAST(year_of_birth AS VARCHAR(4)),'-',
                                                    CAST(month_of_birth AS VARCHAR(2)),'-',
                                                    CAST(day_of_birth AS VARCHAR(2))) AS DATE) > "1900-01-01" 
                                        AND CAST(concat(CAST(year_of_birth AS VARCHAR(4)),'-',
                                                        CAST(month_of_birth AS VARCHAR(2)),'-',
                                                        CAST(day_of_birth AS VARCHAR(2))) AS DATE)  < "2018-10-01" """
            pats_wit_oneoutQuery: str = f"""
                                        SELECT COUNT(DISTINCT(person_id)) 
                                        ###CHECK HERE###
                                        FROM {self.schema},subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table))," 
                                        WHERE toupper({col}) IS NULL OR CAST(toupper({col}) AS CHAR(54)) IN {exclude} """
            patsWithoutRecordsQuery: str = f"""
                                        SELECT COUNT(DISTINCT(person_id))
                                        FROM {self.schema}{self.prefix} PERSON 
                                        WHERE CAST(concat(CAST(year_of_birth AS VARCHAR(4)),'-',
                                                            CAST(month_of_birth AS VARCHAR(2)),'-',
                                                            CAST(day_of_birth AS VARCHAR(2))) AS DATE) > "1900-01-01" 
                                                AND CAST(concat(CAST(year_of_birth AS VARCHAR(4)),'-',
                                                                CAST(month_of_birth AS VARCHAR(2)),'-',
                                                                CAST(day_of_birth AS VARCHAR(2))) AS DATE)  < "2018-10-01"
                                                AND person_id IN (SELECT DISTINCT(person_id)
                                                                ###CHECK HERE### 
                                                                FROM {self.schema},subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table))," 
                                                                WHERE toupper({col}) IS NOT NULL AND CAST(toupper({col}) AS CHAR(54)) NOT IN {exclude} """

        cursor = self.conn.cursor()
        denominator = cursor.execute(denominatorQuery)
        pats_wit_oneout = cursor.execute(pats_wit_oneoutQuery)
        patsWithoutRecords = cursor.execute(patsWithoutRecordsQuery)
        self.conn.close()

        ppwo: int = round((pats_wit_oneout/denominator)*100,4)
        pwse: int = round(((denominator-patsWithoutRecords)/denominator)*100,4)

        # if (ppwo > 1) message(pats_wit_oneout, " of the patients -- ",ppwo,"% of patients -- are missing at least 1 acceptable ",toupper(col)," value in the ",toupper(table)," table.",appendLF=T)
        # if (pwse > 1) message(whatsoever, " of the patients -- ",pwse,"% of patients -- are missing any acceptable ",toupper(col)," value in the ",toupper(table)," table.",appendLF=T)
        # message(pwse, "% of unique patients don't have any '", group,"' record in the ",table, " table.",appendLF=T)

        return pandas.DataFrame({
                                "GROUP": [group],
                                "MISSING PERCENTAGE": [pwse],
                                "MISSING POPULATION": [patsWithoutRecords],
                                "DENOMINATOR": [denominator],
                                "PERCENTAGE": [str(round(pwse,2))+"%"],
                                "TEST_DATE": [datetime.datetime.today().strftime('%m-%d-%Y')],
                                "ORGANIZATION": [self.organization],
                                "CDM": [self.CDM]
                                })

    # WHY IS CONCEPT AN ARG WHEN IT IS ALWAYS EMPTY?
    def isPresentOMOP(self, table: str, col: str, group: str, concept: str = "") -> object:
        exclude: Dict[str,List] = {
                "BP"    : self.getChildConceptsOMOP([45876174,4326744]), # getChildConceptsOMOP returns a list, so no need for brackets; check when testing
                "HR"    : self.getChildConceptsOMOP([4239408]),
                "Height": self.getChildConceptsOMOP([4177340,4087492,4154781,4275188,44804668]),
                "Weight": self.getChildConceptsOMOP([4181041,4184608,45876171,4103471,44804668,4030015]),
                "Smoker": [4310250, 40299112, 40329177, 40298672, 40329167, 42536346, 46270534,
                        4144272, 45879404, 42872410, 40298657, 45883537, 45884038, 4298794,
                        40427925, 40299110, 45878118, 42709996, 45884037, 43530634, 4141786]
            }
        
        if self.DBMS == "postgresql":
            denominatorQuery: str = f"""
                                    SELECT COUNT(DISTINCT(person_id))
                                    FROM {self.schema}{self.prefix} PERSON
                                    WHERE CAST(concat(CAST(year_of_birth AS VARCHAR(4)),'-',
                                                    CAST(month_of_birth AS VARCHAR(2)),'-',
                                                    CAST(day_of_birth AS VARCHAR(2))) AS DATE) > "1900-01-01" 
                                        AND CAST(concat(CAST(year_of_birth AS VARCHAR(4)),'-',
                                                        CAST(month_of_birth AS VARCHAR(2)),'-',
                                                        CAST(day_of_birth AS VARCHAR(2))) AS DATE)  < "2018-10-01" """

            if (concept==""):
                pats_with_oneQuery: str = f"""
                                        SELECT COUNT(DISTINCT(person_id)) 
                                        FROM {self.schema}{table} 
                                        WHERE toupper({col}) IN {exclude[group]} """
            
            else:
                pats_with_oneQuery: str = f"""
                                        SELECT COUNT(DISTINCT(person_id)) 
                                        FROM {self.prefix}{self.schema}{table} 
                                        WHERE toupper({col}) IN (SELECT concept_id
                                                                FROM {self.prefix}{self.schema} CONCEPT 
                                                                WHERE domain_id= {concept} """

        elif self.DBMS == "sql server" or self.DBMS == "redshift":
            denominatorQuery: str = f"""
                                    SELECT COUNT(DISTINCT(person_id))
                                    FROM {self.schema}{self.prefix} PERSON
                                    WHERE CONVERT(DATETIME,
                                                CAST([year_of_birth] AS VARCHAR(4))+'-'+
                                                CAST([month_of_birth] AS VARCHAR(2))+'-'+
                                                CAST([day_of_birth] AS VARCHAR(2))) > "1900-01-01" 
                                        AND CONVERT(DATETIME,
                                                    CAST([year_of_birth] AS VARCHAR(4))+'-'+
                                                    CAST([month_of_birth] AS VARCHAR(2))+'-'+
                                                    CAST([day_of_birth] AS VARCHAR(2)))  < "2018-10-01" """
            if (concept==""):
                pats_with_oneQuery: str = f"""
                                        SELECT COUNT(DISTINCT(person_id)) 
                                        FROM {self.schema}{table} 
                                        WHERE toupper({col}) IN {exclude[group]} """
            else:
                pats_with_oneQuery: str = f"""
                                        SELECT COUNT(DISTINCT(person_id)) 
                                        FROM {self.prefix}{self.schema}{table} 
                                        WHERE toupper({col}) IN (SELECT concept_id 
                                                                FROM {self.prefix}{self.schema} CONCEPT 
                                                                WHERE domain_id= {concept} """

        cursor = self.conn.cursor()
        denominator = cursor.execute(denominatorQuery)
        pats_with_one = cursor.execute(pats_with_oneQuery)
        self.conn.close()

        ppwo: int = round((pats_with_one/denominator)*100,4)
        pwse: int = round(((denominator-pats_with_one)/denominator)*100,4)

        # if (ppwo > 1) message(pats_with_one, " of the patients -- ",ppwo,"% of patients -- have at least 1 acceptable ",toupper(col)," value in the ",toupper(table)," table.",appendLF=T)
        # if (concept==""):
            # message(pwse, "% of unique patients don't have any '", group,"' record in the ",table, " table.",appendLF=T)
        # else:
            # message(pwse, "% of unique patients don't have any '", concept,"' record in the ",table, " table.",appendLF=T)

        return pandas.DataFrame({
                                "GROUP": [group if concept == "" else concept],
                                "MISSING PERCENTAGE": [pwse],
                                "MISSING POPULATION": [denominator-pats_with_one],
                                "DENOMINATOR": [denominator],
                                "PERCENTAGE": [str(round(pwse,2))+"%"],
                                "TEST_DATE": [datetime.datetime.today().strftime('%m-%d-%Y')],
                                "ORGANIZATION": [self.organization],
                                "CDM": [self.CDM]
                                })
    
    def getChildConceptsOMOP(self, conceptId: List[int]) -> List: # supposed to return a list, check when testing
        childConceptsQuery: str = f"""
                                SELECT descendant_concept_id
                                FROM {self.schema}{self.prefix} CONCEPT_ANCESTOR
                                WHERE ancestor_concept_id IN {conceptId} """

        cursor = self.conn.cursor()
        childConcepts = cursor.execute(childConceptsQuery)
        self.conn.close()

        return(childConcepts)