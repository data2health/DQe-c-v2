from orphan import Orphan
from prep import Prep
from query import Query
from diff import Diff

def main():

    details = Prep()
    query = Query(details)

    ## outputs the DQTBL object and generates the reports/tablelist.csv file
    DQTBL = Diff(details, query).getDQTBL()


    #orph = Orphan(details)
    #orph.orphanCalc()

    return False


if __name__ == "__main__":
    main()