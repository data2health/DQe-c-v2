from orphan import Orphan
from missingness import Missingness
from indicator import Indicator
from prep import Prep
from query import Query
from diff import Diff
import pandas as pd

def main():

    query = Query()

    ## outputs the DQTBL object and generates the reports/tablelist.csv file
    diff = Diff(query)
    DQTBL = diff.getDQTBL()
    #DQTBL = pd.read_csv("tables/DQTBL.csv")

    miss = Missingness(DQTBL, query)
    DQTBL = miss.getMissingness()
    #DQTBL = pd.read_csv("reports/missingness.csv")
    
    # run the orphan key calculations
    orph = Orphan(DQTBL, query)
    DQTBL = orph.orphanCalc()

    #orph = Orphan(details)
    #orph.orphanCalc()

    # Missingness(DQTBL, query).get()
    # Indicator(query).get()

    DQTBL.to_csv("reports/DQTBL_report.csv")
    return False


if __name__ == "__main__":
    main()