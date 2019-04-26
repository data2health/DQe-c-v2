from modules.orphan import Orphan
from modules.missingness import Missingness
from modules.indicator import Indicator
from modules.query import Query
from modules.diff import Diff
import pandas as pd
import argparse

def main(config):



    query = Query(config)

    ## Generates the reports/tablelist.csv file
    ## Filters the query.DQTBL object to include only tables that
    ## are present in the live database.
    Diff(query).createDifference()
    print ("Differences has been run")


    ## Adds the missingness statistics to the query.DQTBL object
    Missingness(query).missing()
    print ("Missingness tests have been run")


    # run the orphan key calculations
    # adds the orphan calculations to the query.DQTBL object
    Orphan(query).orphanCalc()
    print ("Orphan Key test has been run")


    # run the Indicator tests
    # new tests can be added to the Indicators.json file
    Indicator(query).get()
    print ("Indicators test has been run")


    query.outputReport(query.DQTBL, "DQe-c_report.csv")

    return False


if __name__ == "__main__":

    description = """
    DQe-c is a data quality testing tool for clinical data repositories.\n
    Currently supported Common Data Models include OMOPv5 and PCORI3.\n
    For more info, see https://github.com/data2health/DQe-c-v2
    """

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c', '--configpath', type=str, default="config.json", help='The path to the configuration file. (default: config.json)')
    
    args = parser.parse_args()
    
    config_file = args.configpath
    
    main(config_file)