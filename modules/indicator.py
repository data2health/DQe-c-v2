import pandas
import datetime
import json
from typing import Dict, List

class Indicator:
    def __init__(self, query: object):
        self.query = query


    def get(self):
            
        indicators_output = []


        ## Pull the CDM specific indicator test
        if self.query.CDM.startswith("OMOP"):
            indicator_tests = json.load(open("tests/indicators_omop.json"))

        elif self.query.CDM.startswith("PCORNET"):
            indicator_tests = json.load(open("tests/indicators_pcornet.json"))

        else:
            raise Exception(f"No indicator.json file for CDM {self.query.CDM}")

        
        ## Iterate through each of the test modules to calculate the percentage of patients
        ## who have at least one records of the indicator
        for test in indicator_tests:
            try:
                if test["concepts"]:

                    ## Since OMOP has the capability to query vocabularies, we got and search all possible
                    ## child terms of our concepts in the list
                    if self.query.CDM.startswith("OMOP"):
                        cur_concepts = self.getChildConceptsOMOP(test["concepts"])

                    else:
                        cur_concepts = test["concepts"]

                    output = self.isPresent(table=test["table"], col=test["col"], group=test["label"], concepts=cur_concepts)
                else:
                    output = self.isPresent(table=test["table"], col=test["col"], group=test["label"])

                indicators_output.append(output)

            except Exception as e:
                output = "Error"
                

        indicators = pandas.concat(indicators_output, ignore_index=True)

        self.query.outputReport(indicators, "indicators.csv")


    

    def isPresent(self, table: str, col: str, group: str, concepts = False) -> object:
        
        if self.query.CDM.startswith("OMOP"):
            patient_id = "person_id"
            patient_table = "PERSON"

        elif self.query.CDM.startswith("PCORNET"):
            patient_id = "PATID"
            patient_table = "DEMOGRAPHIC"
        else:
            raise Exception(f"CDM {self.query.CDM} not configured in indicator")

        denominatorQuery: str = f"""
                            SELECT COUNT(DISTINCT({patient_id})) as count
                            FROM {self.query.prefix}{patient_table} """

        ## If a concept list is present, will check that the column value is in that list
        if concepts:
            pats_with_oneQuery: str = f"""
                                SELECT COUNT(DISTINCT({patient_id})) as count
                                FROM {self.query.prefix}{table} 
                                WHERE {col} IN ({concepts}) """
        
        ## If a concept list is not present, will check that the column value is not null and not nonsense
        else:
            exclude = "'+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI'"

            if self.query.DBMS == "oracle":
                
                pats_with_oneQuery: str = f"""
                                            SELECT COUNT(UNIQUE({patient_id})) as count
                                            FROM {self.query.prefix}{table} 
                                            WHERE {col} IS NOT NULL AND TO_CHAR({col}) NOT IN ({exclude}) """

            
            elif self.query.DBMS in ["sql server", "redshift", "postgresql"]:
                
                pats_with_oneQuery: str = f"""
                                            SELECT COUNT(DISTINCT({patient_id})) as count
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
                                "MISSING_PERCENTAGE": [pwse],
                                "MISSING_POPULATION": [denominator-pats_with_one],
                                "DENOMINATOR": [denominator],
                                "PERCENTAGE": [str(round(pwse,2))+"%"],
                                "TEST_DATE": [datetime.datetime.today().strftime('%m-%d-%Y')],
                                "ORGANIZATION": [self.query.organization],
                                "CDM": [self.query.CDM]
                                })
    
    
    ## This function is designed to use the OMOP vocabulary tables to 
    # search for child terms to the input list of concepts
    def getChildConceptsOMOP(self, conceptId):
        childConceptsQuery: str = f"""
                                SELECT descendant_concept_id
                                FROM {self.query.vocab_prefix}CONCEPT_ANCESTOR
                                WHERE ancestor_concept_id IN ({",".join([str(i) for i in conceptId])}) """

        childConcepts = ",".join([str(i) for i in list(pandas.read_sql(childConceptsQuery, con=self.query.conn)["descendant_concept_id"])])
        return(childConcepts)