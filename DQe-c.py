from orphan import Orphan
from prep import Prep
from query import Query

def main():
    dq = Prep()
    print (dq.CDM)
    
    #orph = Orphan("OMOPV5_0")

    #print (orph.orphanCalc())


if __name__ == "__main__":
    main()