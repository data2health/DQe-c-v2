from orphan import Orphan
from prep import Prep
from query import Query

def main():

    details = Prep()
    query = Query()


    orph = Orphan(details)
    orph.orphanCalc()

    return False


if __name__ == "__main__":
    main()