"""
This code was developed by the University of Washington

"""


from modules.orphan import Orphan
from modules.missingness import Missingness
from modules.indicator import Indicator
from modules.query import Query
from modules.diff import Diff
from modules.network import Network
import pandas as pd
import argparse
from argparse import RawTextHelpFormatter

def main(config, vis):


    ## initiate database connection and tool configurations
    query = Query(config, vis)

    # if -v options are site-only or network-only, the tool skips the modules
    if vis in ["site-only", "network-only"]:
        pass
    else:
        
        ## Generates the reports/tablelist.csv file
        ## Filters the query.DQTBL object to include only tables that
        ## are present in the live database. 
        # Module will quite program if no tables are loaded
        Diff(query).createDifference()
        print ("Differences has been run")


        ## Generates the missingness statistics report
        Missingness(query).missing()
        print ("Missingness tests have been run")


        # run the orphan key calculations
        Orphan(query).orphan()
        print ("Orphan Key test has been run")


        # run the Indicator tests
        # new tests can be added to the Indicators.json file
        Indicator(query).get()
        print ("Indicators test has been run")
        

        #----------------------
        #Add new modules here!
        #For more information on creating modules, see the wiki documentation.
        #----------------------


    if vis in ["none"]:
        pass
    elif vis in ["network-only", "network", "all"]:
        # Builds the network aggregation reports
        Network(query).createAggregateReports()

        # Generates network view HTML report
        Network(query).generateNetworkHTMLReport()
    else:
        # Generates site specific HTML report
        query.generateHTMLReport()




if __name__ == "__main__":

    description = """
    DQe-c is a data quality testing tool for clinical data repositories.
    Currently supported Common Data Models include OMOPv5 and PCORI3.
    For more info, see https://github.com/data2health/DQe-c-v2.
    """

    parser = argparse.ArgumentParser(description=description, formatter_class=RawTextHelpFormatter)
    
    parser.add_argument('-c', '--configpath', type=str, default="config.json", help='The path to the configuration file. (default: config.json)')
    
    parser.add_argument('-v', '--visualization', type=str, default='site', choices=["site", "site-only", "network", "network-only", "none", "all"], \
        help="Options for creating the visualization dashboards. (default: site)\
            \nOptions include:\
            \n\tsite - create dashboard for just this site\
            \n\tsite-only - Only creates the site dashboard without running the modules\
            \n\tnetwork - generate the dashboard to compare data quality tests from all the sites\
            \n\tnetwork-only - generate the network view dashboard without running any modules\
            \n\tall - create both dashboards for site and network view\
            \n\tnone - runs the modules without generating any visualizations")


    args = parser.parse_args()
    
    config_file = args.configpath
    vis_opt = args.visualization
    
    main(config_file, vis_opt)