from orphan import Orphan
from missingness import Missingness
from indicator import Indicator
from query import Query
from diff import Diff
import pandas as pd
import argparse

def main(config):



    query = Query(config)

    ## outputs the DQTBL object and generates the reports/tablelist.csv file
    Diff(query).createDifference()


    ## Adds the missingness statistics to the query.DQTBL  object
    Missingness(query).missing()


    # run the orphan key calculations
    Orphan(query).orphanCalc()


    # run the Indicator tests
    # new tests can be added to the Indicators.json file
    Indicator(query).get()

    
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