from prep import Prep
import pandas
import datetime
import json

class Indicator:
    def __init__(self, query: object):
        self.query = query

    def get(self):
        if self.query.CDM == "PCORNET3" or self.query.CDM == "PCORNET31":
            withoutGenderPCORI      = self.query.withoutdemPCORI(col = "sex", group = "Gender")
            withoutRacePCORI        = self.query.withoutdemPCORI(col = "race", group = "Race")
            withoutEthnicityPCORI   = self.query.withoutdemPCORI(col = "hispanic", group = "Ethnicity")

            withoutMedicationPCORI  = self.query.withoutPCORI(table = "PRESCRIBING", col = "prescribingid", group = "Medication")
            withoutDiagnosisPCORI   = self.query.withoutPCORI(table = "DIAGNOSIS", col = "dx", group = "Diagnosis")
            withoutEncounterPCORI   = self.query.withoutPCORI(table = "ENCOUNTER", col = "enc_type", group = "Encounter")
            withoutWeightPCORI      = self.query.withoutPCORI(table = "VITAL", col = "wt", group = "Weight")
            withoutHeightPCORI      = self.query.withoutPCORI(table = "VITAL", col = "ht", group = "Height")
            withoutBpSysPCORI       = self.query.withoutPCORI(table = "VITAL", col = "systolic", group = "BP")
            withoutBpDiasPCORI      = self.query.withoutPCORI(table = "VITAL", col = "diastolic", group = "BP")
            withoutSmokingPCORI     = self.query.withoutPCORI(table = "VITAL", col = "smoking", group = "Smoking")

            indicators = pandas.concat([withoutGenderPCORI, withoutRacePCORI, withoutEthnicityPCORI, withoutMedicationPCORI,
                                    withoutDiagnosisPCORI, withoutEncounterPCORI, withoutWeightPCORI, withoutHeightPCORI,
                                    withoutBpSysPCORI, withoutBpDiasPCORI, withoutSmokingPCORI], ignore_index=True)


        elif self.query.CDM == "OMOPV5_0" or self.query.CDM == "OMOPV5_2" or self.query.CDM == "OMOPV5_3":
            
            indicators_output = []

            indicator_tests = json.load(open("indicators.json"))

            for test in indicator_tests:
                try:
                    if test["concepts"]:
                        output = self.isPresentOMOP(table=test["table"], col=test["col"], group=test["label"], concepts=self.getChildConceptsOMOP(test["concepts"]))
                    else:
                        output = self.isPresentOMOP(table=test["table"], col=test["col"], group=test["label"])

                    indicators_output.append(output)

                except Exception as e:
                    output = "Error"

            indicators = pandas.concat(indicators_output, ignore_index=True)

        self.query.outputReport(indicators, "indicators_report.csv")


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
                                    FROM {self.prefix}DEMOGRAPHIC
                                    WHERE BIRTH_DATE > 1900-01-01 AND BIRTH_DATE  < 2014-01-01
                                    ) dd
                            WHERE toupper({col}) NOT IN {exclude[group]} """
            whattheyhaveQuery: str = f"""
                                    SELECT DISTINCT(toupper({col}))
                                    FROM   (
                                            SELECT *
                                            FROM {self.prefix}DEMOGRAPHIC
                                            WHERE BIRTH_DATE > 1900-01-01 AND BIRTH_DATE  < 2014-01-01
                                            ) dd
                                    WHERE toupper({col}) NOT IN {exclude[group]} """
        
        cursor = self.conn.cursor()
        denominator = cursor.execute(denominatorQuery)
        notin = cursor.execute(notinQuery)
        whattheyhave = cursor.execute(whattheyhaveQuery)
        self.conn.close()

        d1: int = round((notin/denominator)*100,4)
        
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

    

    # WHY IS CONCEPT AN ARG WHEN IT IS ALWAYS EMPTY?
    def isPresentOMOP(self, table: str, col: str, group: str, concepts = False) -> object:
        
        denominatorQuery: str = f"""
                            SELECT COUNT(DISTINCT(person_id))
                            FROM {self.query.prefix}PERSON """

        ## If a concept list is present, will check that the column value is in that list
        if concepts:
            pats_with_oneQuery: str = f"""
                                SELECT COUNT(DISTINCT(person_id)) 
                                FROM {self.query.prefix}{table} 
                                WHERE {col} IN ({concepts}) """
        
        ## If a concept list is not present, will check that the column value is not null and not nonsense
        else:
            exclude = "'+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI'"

            if self.query.DBMS == "oracle":
                
                pats_with_oneQuery: str = f"""
                                            SELECT COUNT(DISTINCT(person_id))
                                            FROM {self.query.prefix}{table} 
                                            WHERE {col} IS NOT NULL AND TO_CHAR({col}) NOT IN ({exclude}) """

            
            elif self.query.DBMS in ["sql server", "redshift", "postgresql"]:
                
                pats_with_oneQuery: str = f"""
                                            SELECT COUNT(DISTINCT(person_id)) 
                                            FROM {self.query.prefix}{table}
                                            WHERE {col} IS NOT NULL AND CAST({col} AS CHAR(54)) NOT IN ({exclude}) """


        # calculates the number unique patients
        denominator = int(pandas.read_sql(denominatorQuery, con=self.query.conn)["count"])

        # calculates the number of patients with at least one indicator
        pats_with_one = int(pandas.read_sql(pats_with_oneQuery, con=self.query.conn)["count"])

        # percentage of patients with at least one instance of the clinical indicator
        ppwo: int = round((pats_with_one/denominator)*100,4)

        # percentage of patients without at least one of the clinical indicators
        pwse: int = round(((denominator-pats_with_one)/denominator)*100,4)

        return pandas.DataFrame({
                                "GROUP": [group],
                                "MISSING PERCENTAGE": [pwse],
                                "MISSING POPULATION": [denominator-pats_with_one],
                                "DENOMINATOR": [denominator],
                                "PERCENTAGE": [str(round(pwse,2))+"%"],
                                "TEST_DATE": [datetime.datetime.today().strftime('%m-%d-%Y')],
                                "ORGANIZATION": [self.query.organization],
                                "CDM": [self.query.CDM]
                                })
    
    
    def getChildConceptsOMOP(self, conceptId):
        childConceptsQuery: str = f"""
                                SELECT descendant_concept_id
                                FROM {self.query.vocab_prefix}CONCEPT_ANCESTOR
                                WHERE ancestor_concept_id IN ({",".join([str(i) for i in conceptId])}) """

        childConcepts = ",".join([str(i) for i in list(pandas.read_sql(childConceptsQuery, con=self.query.conn)["descendant_concept_id"])])
        return(childConcepts)



    def missing_variable(self, group, concept = "", ):
        return pandas.DataFrame({
            "GROUP": [group if concept == "" else concept],
            "MISSING PERCENTAGE": [100],
            "MISSING POPULATION": [0],
            "DENOMINATOR": [0],
            "PERCENTAGE": [str(round(0.00,2))+"%"],
            "TEST_DATE": [datetime.datetime.today().strftime('%m-%d-%Y')],
            "ORGANIZATION": [self.query.organization],
            "CDM": [self.query.CDM]
            })