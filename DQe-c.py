from orphan import Orphan
from prep import Prep
from query import Query
from missingness import Missingness
from diff import Diff

def main():

    details = Prep()
    query = Query(details)
    DQTBL = Diff(details, query).DQTBL_diff()


    orph = Orphan(details, query)
    miss = Missingness(details, DQTBL)
    orph.orphanCalc()

    return False


if __name__ == "__main__":
    main()