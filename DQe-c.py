from orphan import Orphan
from prep import Prep
from query import Query
<<<<<<< HEAD
from missingness import Missingness
=======
>>>>>>> aacbff82a74c6c7e6bc5ca48985cc27b6b5b26d7
from diff import Diff

def main():

    details = Prep()
    query = Query(details)
<<<<<<< HEAD
    DQTBL = Diff(details, query).DQTBL_diff()
=======
>>>>>>> aacbff82a74c6c7e6bc5ca48985cc27b6b5b26d7

    ## outputs the DQTBL object and generates the reports/tablelist.csv file
    DQTBL = Diff(details, query).getDQTBL()

<<<<<<< HEAD
    orph = Orphan(details, query)
    miss = Missingness(details, DQTBL)
    orph.orphanCalc()
=======

    #orph = Orphan(details)
    #orph.orphanCalc()
>>>>>>> aacbff82a74c6c7e6bc5ca48985cc27b6b5b26d7

    return False


if __name__ == "__main__":
    main()