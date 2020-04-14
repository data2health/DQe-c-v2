import pandas
import datetime
import json
from typing import Dict, List

class Vocab:
    def __init__(self, query: object):

        self.query = query
        self.DQTBL = self.query.DQTBL

    def vocabulary(self):
        if self.query.CDM.startswith("OMOP"):
            self.vocabulary_omop()
        else:
            pass


    def vocabulary_omop(self):

        vocabulary_tests = json.load(open("tests/vocabulary_omop.json"))
        vocab_results = []

        for test in vocabulary_tests:
            try:
                results = self.vocabulary_query_omop(test["table"], test["column"])
                vocab_results.append(results)
            except pandas.io.sql.DatabaseError:
                pass
        
        vocabulary = pandas.concat(vocab_results, ignore_index=True)
        
        self.query.outputReport(vocabulary, "vocabulary.csv")


    
    def vocabulary_query_omop(self, table: str, col: str):
        
        query = f"""
            SELECT voc.vocabulary_name as vocabulary, COUNT(tab.{col}) as count
            FROM 
                {self.query.prefix}{table} tab 
                    LEFT JOIN {self.query.vocab_prefix}concept con
                        ON con.concept_id = tab.{col}
                    LEFT JOIN {self.query.vocab_prefix}vocabulary voc
                        on con.vocabulary_id = voc.vocabulary_id
            GROUP BY voc.vocabulary_name
        """

        vocab_counts = pandas.read_sql(query, con=self.query.conn)

        vocab_counts["percent"] = vocab_counts["count"].apply(lambda x:\
            (float(x)/float(sum(vocab_counts["count"])))*100)

        vocab_counts["TabNam"] = table
        vocab_counts["ColNam"] = col
        vocab_counts["TEST_DATE"] = datetime.datetime.today().strftime('%m-%d-%Y')

        return vocab_counts
        
        