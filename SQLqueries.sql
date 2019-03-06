-- From prep.R file. 

-- Database: "SQLServer" 
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

------------------------------------------------------------------------------------------------

-- From funcs_PCORNET3 file


-- SQL == "SQLServer"
-- A function to count patients without a given parameter within the source patient table

-- set the denominator
SELECT COUNT(DISTINCT(PATID))
FROM DEMOGRAPHIC
WHERE BIRTH_DATE > '",ref_date1,"' AND BIRTH_DATE  < '", ref_date2,"'
    
-- count patients with unacceptable values for the given column and table
--not in
SELECT COUNT(PATID)
FROM
    (
        SELECT *
        FROM ",schema,prefix,"DEMOGRAPHIC
        WHERE BIRTH_DATE > '",ref_date1,"'
            AND BIRTH_DATE  < '",ref_date2,"'
    ) dd
WHERE ",toupper(col), " NOT IN ('",paste(list,collapse = "','"),"')

--what they have
SELECT DISTINCT(",toupper(col),")
FROM
    (
        SELECT *
        FROM ",schema,prefix,"DEMOGRAPHIC
        WHERE BIRTH_DATE > '",ref_date1,"' AND BIRTH_DATE  < '",ref_date2,"'
    ) dd
WHERE ",toupper(col), " NOT IN ('",paste(list,collapse = "','"),"')
    

-- a function to count patients that are not available in the list of certain condition/drug/lab/...
SELECT COUNT(DISTINCT(PATID))
FROM ",schema,prefix," DEMOGRAPHIC
WHERE BIRTH_DATE > '",ref_date1,"' AND BIRTH_DATE  < '",ref_date2,"'

-- patients with at least one value out of what we want
SELECT COUNT(DISTINCT(PATID))
FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table)),"
WHERE ",toupper(col), " IS NULL OR CAST(",toupper(col), " AS CHAR(54)) IN  ('",paste(list,collapse = "','"),"')

-- patients who don't have any records whatsoever
-- we calculate valid patients first
SELECT COUNT(DISTINCT(PATID))
FROM ",schema,prefix," DEMOGRAPHIC
WHERE BIRTH_DATE > '",ref_date1,"' AND BIRTH_DATE  < '",ref_date2,"'
                                   AND PATID IN (
                                                    SELECT DISTINCT(PATID)
                                                    FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table)),"
                                                    WHERE ",toupper(col), " IS NOT NULL AND CAST(",toupper(col), " AS CHAR(54)) NOT IN  ('",paste(list,collapse = "','"),"')
    
-- SQL == "Oracle"

-- set the denominator
SELECT COUNT(DISTINCT(PATID))
FROM DEMOGRAPHIC
WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd') AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')
      
-- count patients with unacceptable values for the given column and table
SELECT COUNT(PATID)
FROM(
        SELECT *
        FROM ",schema,prefix," DEMOGRAPHIC
        WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd') AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')
    )
WHERE ",toupper(col), " NOT IN ('",paste(list,collapse = "','"),"')

-- what they have      
SELECT DISTINCT(",toupper(col),")
FROM(
        SELECT *
        FROM ",schema,prefix," DEMOGRAPHIC
        WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd') AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')
    )
WHERE ",toupper(col), " NOT IN ('",paste(list,collapse = "','"),"')

-- a function to count patients that are not available in the list of certain condition/drug/lab/...
-- set the denominator
SELECT COUNT(DISTINCT(PATID))
FROM ",schema,prefix," DEMOGRAPHIC
WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd') AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')

-- patients with at least one value out of what we want
SELECT COUNT(DISTINCT(PATID))
FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table)),"
WHERE ",toupper(col), " IS NULL OR TO_CHAR(",toupper(col),") IN  ('",paste(list,collapse = "','"),"')

-- patients who don't have any records whatsoever
-- we calculate valid patients first
SELECT COUNT(DISTINCT(PATID))
FROM ",schema,prefix," DEMOGRAPHIC
WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd') AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')
        AND PATID IN(
                        SELECT DISTINCT(PATID)
                        FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table)),"
                        WHERE ",toupper(col), " IS NOT NULL AND TO_CHAR(",toupper(col),") NOT IN  ('",paste(list,collapse = "','"),"')
                    )

-- a function to count orphan foriegn keys
SELECT COUNT(DISTINCT(",toupper(col),"))
FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table2)),"
WHERE ",toupper(col)," NOT IN(
                                SELECT DISTINCT(",toupper(col),")
                                FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table1)),"
                             )

------------------------------------------------------------------------------------------------

-- From funcs_OMOP5

-- a function to count patients without a given parameter within the source patient table
-- SQL == "SQLServer" or "Redshift"

SELECT COUNT(DISTINCT(person_id))
FROM ", schema,prefix," PERSON
WHERE CONVERT(DATETIME,
                CAST([year_of_birth] AS VARCHAR(4))+'-'+
                CAST([month_of_birth] AS VARCHAR(2))+'-'+
                CAST([day_of_birth] AS VARCHAR(2))) > '", ref_date1,"' 
                AND 
                CONVERT(DATETIME,
                    CAST([year_of_birth] AS VARCHAR(4))+'-'+
                    CAST([month_of_birth] AS VARCHAR(2))+'-'+
                    CAST([day_of_birth] AS VARCHAR(2)))  < '", ref_date2,"'
    
    
-- count patients with unacceptable values for the given column and table
-- notin
SELECT COUNT(person_id) 
FROM(
        SELECT * 
        FROM ",schema,prefix," PERSON
        WHERE CONVERT(DATETIME,
                        CAST([year_of_birth] AS VARCHAR(4))+'-'+
                        CAST([month_of_birth] AS VARCHAR(2))+'-'+
                        CAST([day_of_birth] AS VARCHAR(2))) > '", ref_date1,"' 
                        AND 
                        CONVERT(DATETIME,
                            CAST([year_of_birth] AS VARCHAR(4))+'-'+
                            CAST([month_of_birth] AS VARCHAR(2))+'-'+
                            CAST([day_of_birth] AS VARCHAR(2)))  < '", ref_date2,"'
    ) dd
WHERE ", toupper(col), " NOT IN (",paste(list, collapse=","),")

-- what they have    
SELECT DISTINCT(",toupper(col),") 
FROM(
        SELECT * 
        FROM ",schema,prefix," PERSON 
        WHERE CONVERT(DATETIME,
                        CAST([year_of_birth] AS VARCHAR(4))+'-'+
                        CAST([month_of_birth] AS VARCHAR(2))+'-'+
                        CAST([day_of_birth] AS VARCHAR(2))) > '",ref_date1,"' 
                        AND 
                        CONVERT(DATETIME,
                            CAST([year_of_birth] AS VARCHAR(4))+'-'+
                            CAST([month_of_birth] AS VARCHAR(2))+'-'+
                            CAST([day_of_birth] AS VARCHAR(2)))  < '", ref_date2,"'
    ) dd
WHERE ", toupper(col), " NOT IN (",paste(list, collapse=","),")

-- a function to count patients that are not available in the list of certain condition/drug/lab/...
-- set the denominator
SELECT COUNT(DISTINCT(person_id))
FROM ", schema,prefix," PERSON
WHERE CONVERT(DATETIME,
                CAST([year_of_birth] AS VARCHAR(4))+'-'+
                CAST([month_of_birth] AS VARCHAR(2))+'-'+
                CAST([day_of_birth] AS VARCHAR(2))) > '", ref_date1,"' 
                AND 
                CONVERT(DATETIME,
                    CAST([year_of_birth] AS VARCHAR(4))+'-'+
                    CAST([month_of_birth] AS VARCHAR(2))+'-'+
                    CAST([day_of_birth] AS VARCHAR(2)))  < '", ref_date2,"'

-- patients with at least one value out of what we want
SELECT COUNT(DISTINCT(person_id))
FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table))," 
WHERE ",toupper(col), " IS NULL OR CAST(",toupper(col), " AS CHAR(54)) IN  ('",paste(list,collapse = "','"),"')

-- patients who don't have any records whatsoever
-- we calculate valid patients first
SELECT COUNT(DISTINCT(person_id))
FROM ",schema,prefix," PERSON 
WHERE CONVERT(DATETIME,
                CAST([year_of_birth] AS VARCHAR(4))+'-'+
                CAST([month_of_birth] AS VARCHAR(2))+'-'+
                CAST([day_of_birth] AS VARCHAR(2))) > '", ref_date1,"' 
                AND
                CONVERT(DATETIME,
                    CAST([year_of_birth] AS VARCHAR(4))+'-'+
                    CAST([month_of_birth] AS VARCHAR(2))+'-'+
                    CAST([day_of_birth] AS VARCHAR(2)))  < '", ref_date2,"'
                    AND person_id IN(
                                        SELECT DISTINCT(person_id) 
                                        FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table))," 
                                        WHERE ",toupper(col), " IS NOT NULL AND CAST(",toupper(col), " AS CHAR(54)) 
                                                                NOT IN  ('",paste(list,collapse = "','"),"')
                                    )

-- a function to find all the patients who do not have a valid variable in their record
-- set the denominator
SELECT COUNT(DISTINCT(person_id))
FROM ", schema,prefix," PERSON
WHERE CONVERT(DATETIME,
                CAST([year_of_birth] AS VARCHAR(4))+'-'+
                CAST([month_of_birth] AS VARCHAR(2))+'-'+
                CAST([day_of_birth] AS VARCHAR(2))) > '", ref_date1,"' 
                AND 
                CONVERT(DATETIME,
                    CAST([year_of_birth] AS VARCHAR(4))+'-'+
                    CAST([month_of_birth] AS VARCHAR(2))+'-'+
                    CAST([day_of_birth] AS VARCHAR(2)))  < '", ref_date2,"'
    
-- patients with at least one of the expected values
-- if concept == ""
SELECT COUNT(DISTINCT(person_id)) 
FROM ",schema,table," 
WHERE ",toupper(col), " IN  (",paste(list,collapse = ","),")

-- else
SELECT COUNT(DISTINCT(person_id)) 
FROM ",prefix,schema,table," 
WHERE ",toupper(col), " IN(
                            SELECT concept_id 
                            FROM ",prefix,schema,"CONCEPT 
                            WHERE domain_id='",concept,"')

-- a function to find all the descendants of the input OMOP concept codes from the concept ancestor table
SELECT descendant_concept_id
FROM ", schema,prefix," CONCEPT_ANCESTOR
WHERE ancestor_concept_id IN ('",paste(concept_id,collapse = "','"),"')

-- SQL == "Oracle"
-- set the denominator
SELECT COUNT(DISTINCT(PATID))
FROM DEMOGRAPHIC
WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd') AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')
      
-- count patients with unacceptable values for the given column and table
-- not in
SELECT COUNT(PATID)
FROM(
        SELECT *
        FROM ",schema,prefix," DEMOGRAPHIC
        WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd')
                AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')
    )
WHERE ",toupper(col), " NOT IN ('",paste(list,collapse = "','"),"')

-- what they have
SELECT DISTINCT(",toupper(col),")
FROM(
        SELECT *
        FROM ",schema,prefix," DEMOGRAPHIC
        WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd')
                AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')
    )
WHERE ",toupper(col), " NOT IN ('",paste(list,collapse = "','"),"')

-- a function to count patients that are not available in the list of certain condition/drug/lab/...
-- set the denominator
SELECT COUNT(DISTINCT(PATID))
FROM ",schema,prefix," DEMOGRAPHIC
WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd')
        AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')

-- patients with at least one value out of what we want
SELECT COUNT(DISTINCT(PATID))
FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table)),"
WHERE ",toupper(col), " IS NULL OR TO_CHAR(",toupper(col),") IN  ('",paste(list,collapse = "','"),"')

-- patients who don't have any records whatsoever
-- we calculate valid patients first
SELECT COUNT(DISTINCT(PATID))
FROM ",schema,prefix," DEMOGRAPHIC
WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd')
        AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')
        AND PATID IN(
                        SELECT DISTINCT(PATID)
                        FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table)),"
                        WHERE ",toupper(col), " IS NOT NULL AND TO_CHAR(",toupper(col),") NOT IN  ('",paste(list,collapse = "','"),"')
                    )

-- SQL == "PostgreSQL"
-- set the denominator
SELECT COUNT(DISTINCT(person_id))
FROM ", schema,prefix," PERSON
WHERE CAST(concat(
                CAST(year_of_birth AS VARCHAR(4)),'-',
                CAST(month_of_birth AS VARCHAR(2)),'-',
                CAST(day_of_birth AS VARCHAR(2))) AS DATE) > '", ref_date1,"' 
      AND 
      CAST(concat(
                CAST(year_of_birth AS VARCHAR(4)),'-',
                CAST(month_of_birth AS VARCHAR(2)),'-',
                CAST(day_of_birth AS VARCHAR(2))) AS DATE)  < '", ref_date2,"'

      
-- count patients with unacceptable values for the given column and table
-- not in
SELECT COUNT(person_id) 
FROM(
        SELECT * 
        FROM ",schema,prefix," PERSON
        WHERE CAST(concat(
                        CAST(year_of_birth AS VARCHAR(4)),'-',
                        CAST(month_of_birth AS VARCHAR(2)),'-',
                        CAST(day_of_birth AS VARCHAR(2))) AS DATE) > '", ref_date1,"' 
              AND 
              CAST(concat(
                        CAST(year_of_birth AS VARCHAR(4)),'-',
                        CAST(month_of_birth AS VARCHAR(2)),'-',
                        CAST(day_of_birth AS VARCHAR(2))) AS DATE)  < '", ref_date2,"'
    ) dd
WHERE ", toupper(col), " NOT IN (",paste(list, collapse=","),")
    
-- what they have 
SELECT DISTINCT(",toupper(col),") 
FROM(
        SELECT * 
        FROM ",schema,prefix,"PERSON 
        WHERE CAST(concat(
                        CAST(year_of_birth AS VARCHAR(4)),'-',
                        CAST(month_of_birth AS VARCHAR(2)),'-',
                        CAST(day_of_birth AS VARCHAR(2))) AS DATE) > '",ref_date1,"' 
              AND 
              CAST(concat(
                        CAST(year_of_birth AS VARCHAR(4)),'-',
                        CAST(month_of_birth AS VARCHAR(2)),'-',
                        CAST(day_of_birth AS VARCHAR(2))) AS DATE)  < '", ref_date2,"'
    ) dd
WHERE ", toupper(col), " NOT IN (",paste(list, collapse=","),")

-- a function to count patients that are not available in the list of certain condition/drug/lab/...
-- set the denominator
SELECT COUNT(DISTINCT(person_id))
FROM ", schema,prefix," PERSON
WHERE CAST(concat(
                CAST(year_of_birth AS VARCHAR(4)),'-',
                CAST(month_of_birth AS VARCHAR(2)),'-',
                CAST(day_of_birth AS VARCHAR(2))) AS DATE) > '", ref_date1,"' 
      AND 
      CAST(concat(
                CAST(year_of_birth AS VARCHAR(4)),'-',
                CAST(month_of_birth AS VARCHAR(2)),'-',
                CAST(day_of_birth AS VARCHAR(2))) AS DATE)  < '", ref_date2,"'

-- patients with at least one value out of what we want
SELECT COUNT(DISTINCT(person_id))
FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table))," 
WHERE ",toupper(col), " IS NULL OR CAST(",toupper(col), " AS CHAR(54)) IN  ('",paste(list,collapse = "','"),"')

-- patients who don't have any records whatsoever
-- we calculate valid patients first
SELECT COUNT(DISTINCT(person_id))
FROM ",schema,prefix," PERSON 
WHERE CAST(concat(
                CAST(year_of_birth AS VARCHAR(4)),'-',
                CAST(month_of_birth AS VARCHAR(2)),'-',
                CAST(day_of_birth AS VARCHAR(2))) AS DATE) > '", ref_date1,"' 
      AND 
      CAST(concat(
                CAST(year_of_birth AS VARCHAR(4)),'-',
                CAST(month_of_birth AS VARCHAR(2)),'-',
                CAST(day_of_birth AS VARCHAR(2))) AS DATE)  < '", ref_date2,"'
                AND 
                person_id IN(
                                SELECT DISTINCT(person_id) 
                                FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table))," 
                                WHERE ",toupper(col), " IS NOT NULL AND CAST(",toupper(col), " AS CHAR(54)) 
                                NOT IN  ('",paste(list,collapse = "','"),"'))


-- a function to find all the patients who do not have a valid variable in their record
-- set the denominator
SELECT COUNT(DISTINCT(person_id))
FROM ", schema,prefix," PERSON
WHERE CAST(concat(
                CAST(year_of_birth AS VARCHAR(4)),'-',
                CAST(month_of_birth AS VARCHAR(2)),'-',
                CAST(day_of_birth AS VARCHAR(2))) AS DATE) > '", ref_date1,"' 
      AND 
      CAST(concat(
                CAST(year_of_birth AS VARCHAR(4)),'-',
                CAST(month_of_birth AS VARCHAR(2)),'-',
                CAST(day_of_birth AS VARCHAR(2))) AS DATE)  < '", ref_date2,"'
      
-- patients with at least one of the expected values
-- if concept == ""
SELECT COUNT(DISTINCT(person_id)) 
FROM ",schema,table," 
WHERE ",toupper(col), " IN  (",paste(list,collapse = ","),")

-- else 
SELECT COUNT(DISTINCT(person_id)) 
FROM ",prefix,schema,table," 
WHERE ",toupper(col), " IN(
                            SELECT concept_id 
                            FROM ",prefix,schema,"CONCEPT 
                            WHERE domain_id='",concept,"')

-- a function to find all the descendants of the input OMOP concept codes from the concept ancestor table
SELECT descendant_concept_id
FROM ", schema,prefix," CONCEPT_ANCESTOR
WHERE ancestor_concept_id IN ('",paste(concept_id,collapse = "','"),"')

-- a function to count orphan foriegn keys
SELECT COUNT(DISTINCT(",toupper(col),"))
FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table2))," 
WHERE ",toupper(col)," NOT IN(
                                SELECT DISTINCT(",toupper(col),")
                                FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table1)),")