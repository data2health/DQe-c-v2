"""
expected input: not sure
expected output: indicators.csv
"""
from prep import Prep
import pandas, datetime

class Indicator:
    def __init__(self, DQTBL: object, query: object):
        self.DQTBL = DQTBL
        self.query = query
        self.organization = Prep().organization
        self.CDM = Prep().CDM
        self.conn = Prep().conn

    ## returns missingness.csv after calculating missingness for each table 
    def get(self):
        # indicators = self.DQTBL.apply(self.query.missingnessCalc, axis=1)
        pass
        # return indicators.to_csv("reports/indicators.csv")

    def queryInd(self):
        indicator = pandas.DataFrame()

        indicator["GROUP"] = pandas.read_sql("SQL query", con=self.conn).values[0][0]
        indicator["MISSING PERCENTAGE"] = pandas.read_sql("SQL query", con=self.conn).values[0][0]
        indicator["MISSING POPULATION"] = pandas.read_sql("SQL query", con=self.conn).values[0][0]
        indicator["DENOMINATOR"] = pandas.read_sql("SQL query", con=self.conn).values[0][0]
        indicator["PERCENTAGE"] = []
        indicator["TEST_DATE"] = datetime.datetime.today().strftime('%m-%d-%Y')
        indicator["ORGANIZATION"] = self.organization
        indicator["CDM"] = self.CDM

        return indicator


#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

# a function to count patients without a given parameter within the source patient table

# if (DBMS == "sql server") {  
def withoutdem(self, table,col,list,ref_date1 = "1900-01-01", ref_date2=datetime.datetime.today()):
    denominator = pandas.read_sql("SELECT COUNT(DISTINCT(PATID)) FROM DEMOGRAPHIC WHERE BIRTH_DATE > '",ref_date1,"' AND BIRTH_DATE  < '",
                                     ref_date2,"'", con=self.conn)
    notin = pandas.read_sql("SELECT COUNT(PATID) FROM (SELECT * FROM ",schema,prefix,"DEMOGRAPHIC WHERE BIRTH_DATE > '",ref_date1,"' AND BIRTH_DATE  < '",
                               ref_date2,"') dd WHERE ",
                               col, " NOT IN ('",paste(list,collapse = "','"),"')", con=self.conn)
    whattheyhave = pandas.read_sql("SELECT DISTINCT(",col,") FROM (SELECT * FROM ",schema,prefix,"DEMOGRAPHIC WHERE BIRTH_DATE > '",ref_date1,"' AND BIRTH_DATE  < '",
                                      ref_date2,"') dd WHERE ",
                                      col, " NOT IN ('",paste(list,collapse = "','"),"')", con=self.conn)
    d1 = round((notin/denominator)*100,4)
    print(d1, f"""% of patients born between ",{ref_date1}," and ",{ref_date2}, " are missing ", {list.name}," information.""")
    if (d1 > 0): print(f"""{notin} of the {denominator} patients born between {ref_date1} and {ref_date2} don't have an acceptable {list.name} record in the {df.name} table.""")
    if (d1 > 0): print(f"""Unacceptable values in column {col} are {whattheyhave}.""")
    output = pandas.DataFrame({ "group" : [list],
                                "missing percentage" : [d1],
                                "missing population" : [notin],
                                "denominator" : [denominator] })
    return output
  
def without(table,col,list,ref_date1 = "1900-01-01", ref_date2=datetime.datetime.today()):
    denominator = pandas.read_sql("SELECT COUNT(DISTINCT(PATID)) FROM ",schema,prefix,"DEMOGRAPHIC WHERE BIRTH_DATE > '",ref_date1,"' AND BIRTH_DATE  < '",ref_date2,"'", con=self.conn)
    pats_wit_oneout = pandas.read_sql("SELECT COUNT(DISTINCT(PATID)) FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table))," WHERE ",toupper(col), " IS NULL OR CAST(",toupper(col), " AS CHAR(54)) IN  ('",paste(list,collapse = "','"),"')", con=self.conn)
    ppwo = round((pats_wit_oneout/denominator)*100,4)
    if (ppwo > 1): print(pats_wit_oneout, " of the patients -- ",ppwo,"% of patients -- are missing at least 1 acceptable ",toupper(col)," value in the ",toupper(table)," table.",appendLF=T)
    whatsoever = pandas.read_sql("SELECT COUNT(DISTINCT(PATID)) FROM ",schema,prefix,"DEMOGRAPHIC WHERE BIRTH_DATE > '",ref_date1,"' AND BIRTH_DATE  < '",ref_date2,"'"," AND PATID IN (SELECT DISTINCT(PATID) FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table))," WHERE ",toupper(col), " IS NOT NULL AND CAST(",toupper(col), " AS CHAR(54)) NOT IN  ('",paste(list,collapse = "','"),"'))", con=self.conn)
    pwse = round(((denominator-whatsoever)/denominator)*100,4)
    if (pwse > 1): print(whatsoever, " of the patients -- ",pwse,"% of patients -- are missing any acceptable ",toupper(col)," value in the ",toupper(table)," table.",appendLF=T)
    print(pwse, "% of unique patients don't have any '", list.name,"' record in the ",df.name, " table.",appendLF=T)
    output <- pandas.DataFrame({ "group" : [list],
                                 "missing percentage" : [pwse],
                                 "missing population" : [whatsoever],
                                 "denominator" : [denominator] })
    return output

# if (SQL == "Oracle") {
def withoutdem(table,col,list,ref_date1 = "1900-01-01", ref_date2=datetime.datetime.today()):
    denominator <- dbGetQuery(conn,
                                paste0("SELECT COUNT(DISTINCT(PATID)) FROM DEMOGRAPHIC WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd') AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')"))
    notin <- dbGetQuery(conn,
                          paste0("SELECT COUNT(PATID) FROM (SELECT * FROM ",schema,prefix,"DEMOGRAPHIC WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd') AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')) WHERE ",
                                 toupper(col), " NOT IN ('",paste(list,collapse = "','"),"')"))
    whattheyhave <- dbGetQuery(conn,
                                 paste0("SELECT DISTINCT(",toupper(col),") FROM (SELECT * FROM ",schema,prefix,"DEMOGRAPHIC WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd') AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')) WHERE ",
                                        toupper(col), " NOT IN ('",paste(list,collapse = "','"),"')"))
    d1 <- round((notin/denominator)*100,4)
    message(d1, "% of patients born between ",ref_date1," and ",ref_date2, " are missing ", list.name," information.",appendLF=T)
    if (d1 > 0) message(notin, " of the ",denominator, " patients born between ",ref_date1," and ",ref_date2, " don't have an acceptable ", toupper(list.name), " record in the ",toupper(df.name), " table.",appendLF=T)
    if (d1 > 0) message("Unacceptable values in column ", toupper(col), " are ",whattheyhave,".",appendLF=T)
    output <- data.frame("group"=list.name, "missing percentage" = as.numeric(d1), "missing population"= as.numeric(notin), "denominator"= as.numeric(denominator))
    return(output)

def without(table,col,list,ref_date1 = "1900-01-01", ref_date2=datetime.datetime.today()):
    denominator <- dbGetQuery(conn,
                            paste0("SELECT COUNT(DISTINCT(PATID)) FROM ",schema,prefix,"DEMOGRAPHIC WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd') AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd')"))
    pats_wit_oneout <- dbGetQuery(conn,
                                paste0("SELECT COUNT(DISTINCT(PATID)) FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table))," WHERE ",toupper(col), " IS NULL OR TO_CHAR(",toupper(col),") IN  ('",paste(list,collapse = "','"),"')")
    )
    ppwo <- round((pats_wit_oneout/denominator)*100,4)
    if (ppwo > 1) message(pats_wit_oneout, " of the patients -- ",ppwo,"% of patients -- are missing at least 1 acceptable ",toupper(col)," value in the ",toupper(table)," table.",appendLF=T)
    whatsoever <- dbGetQuery(conn,
                            paste0("SELECT COUNT(DISTINCT(PATID)) FROM ",schema,prefix,"DEMOGRAPHIC WHERE BIRTH_DATE > TO_DATE('",ref_date1,"', 'yyyy-mm-dd') AND BIRTH_DATE  < TO_DATE('",ref_date2,"', 'yyyy-mm-dd') AND PATID IN (SELECT DISTINCT(PATID) FROM ",schema,subset(tbls2$Repo_Tables,tbls2$CDM_Tables == tolower(table))," WHERE ",toupper(col), " IS NOT NULL AND TO_CHAR(",toupper(col),") NOT IN  ('",paste(list,collapse = "','"),"'))")
    )
    pwse <- round(((denominator-whatsoever)/denominator)*100,4)
    if (pwse > 1) message(whatsoever, " of the patients -- ",pwse,"% of patients -- are missing any acceptable ",toupper(col)," value in the ",toupper(table)," table.",appendLF=T)
    message(pwse, "% of unique patients don't have any '", list.name,"' record in the ",df.name, " table.",appendLF=T)
    output <- data.frame("group"=list.name, "missing percentage" = as.numeric(pwse), "missing population"=as.numeric(whatsoever),"denominator"=as.numeric(denominator))
    return(output)