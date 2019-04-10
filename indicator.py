"""
expected input: not sure
expected output: indicators.csv
"""

# configs 
## table for person reference: person_table=person
## column for person ids: person_ref=person_id

# checking for key clincial features or patient features
# without_indiator(table, col, list="valid concepts")

# without_records(table, col=person_ref)
## check for what percentage of persons in person_table do not have a record in input table

# get_indicators()
## BP=['1','2']
## output_df["BP"] = without_indicator("measurement", "measurement_concept_id", list=BP)
