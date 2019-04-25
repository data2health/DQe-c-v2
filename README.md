# DQe-c-v2
Re-engineering the DQe-c Data Quality package

# Description
We are re-building the original DQe-c from scratch in python to make it more scalable and adaptable.

## Proposed high level workflow
![workflow](images/DQe-c-v2_workflow_two.png)


## Module descriptions
### config.json
Collects the credentials and connection details from the user. These are used to later query the database for the various tests.
### Prep.py
This module starts the process by loading the CDM reference file and calculating the size of each table in the repository. The object DQTBL is created in this module, including only the tables and columns that are actually present in the database. All tables and columns that are supposed to be present (part of the reference CDM) but are not present are reported in tablelist.csv
### Query.py
This module loads the DQTBL object from Prep and loads all the necessary sql queries. Having all the sql queries in one spot makes managing the different types of DBMS easier. This module serves as the "base" from which to run the different module tests. As more modules are created, they should be built using this module.
### Missingingness.py
This module runs through the whole repository and calculates the percent missingness for each column of each table. We check for nonsense values (%, #, !, @, etc) as well as NULL values.
### Orphan.py
Checks for orphan keys, or foreign keys that are not present in the primary table. An example is to check that all the person_id values in observation or measurement are present in the person table.
### Indicators.py
Calculates the percentage of patients that don't have key clinical indicators in their records. This includes measurements like Blood Pressure, Heart Rate, or White Blood Cell count. We also include overall record completeness checks, like what percent of patients don't have a visit, a medication, or an observation associated with their record.
