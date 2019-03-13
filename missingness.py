"""
expected input: DQTBL
expected output: missingness.csv
"""

import pandas

class Missingness:
    def __init__(self, connection, DQTBL: object):
        with open('config.json') as json_file:
            self.config = json.load(json_file)
        self.DBMS: str = self.config["DBMS"].lower()
        self.freqTab = pandas.DataFrame({ "TEST_DATE": [""],
                                          "FREQ": [""],
                                          "UNIQUE_FREQ": [""],
                                          "MS1_FREQ": [""],
                                          "MS2_FREQ": [""],
                                          "MSs_PERCENTAGE": [""]
                                        })
        # once freqTab is populated, we can merge DQTBL with freqTab to proude missingness.csv 
        # self.missingnessDF = pandas.concat([DQTBL, freqTab], axis=1)


# what do some of these variables mean? can we go through the sql queries?

for i in range(1, length(DQTBL.TabNam.unique())):
    NAM = DQTBL.TabNam.unique()[i]
    NAM_Repo = as.character(tbls2[(tbls2$CDM_Tables == NAM),"Repo_Tables"])
    id.NAM = which(DQTBL.TabNam == NAM)
    id.repotabs = which(repotabs$TABLE_NAME == NAM_Repo)
    NAMTB = DQTBL[id.NAM,]
    REPOTB = repotabs[id.repotabs,]
    
    for j in range(1:dim(REPOTB)[1]):
        col = REPOTB.COLUMN_NAME[j]
    
        if (self.DBMS == "sql server" or self.DBMS == "redshift") :
            MS1Freq = as.numeric(dbGetQuery(conn, paste0("SELECT COUNT('", col,"') FROM ",schema,NAM_Repo," WHERE [", col, "] IS NULL OR CAST(", col, " AS VARCHAR) IN ('')")))
            
            MS2_FRQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT('", col,"') FROM ",schema,NAM_Repo," WHERE CAST(", col, " AS VARCHAR) IN ('+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI')")))
           
            self.DQTBL.MS1_FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, MS1Freq, DQTBL$MS1_FRQ )
            self.DQTBL.MS2_FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, MS2_FRQ, DQTBL$MS2_FRQ )

            # freqTab=freqTab.append({"MS1_FREQ":value, "MS2_FREQ":value}, ignore_index=True) if (DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM) else ...
        
        elif self.DBMS == "oracle":
            MS1_FRQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT('", col,"') FROM ",schema,NAM_Repo," WHERE ", col, " IS NULL OR TO_CHAR(", col, ") IN ('')")))
            self.DQTBL.MS1_FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, MS1_FRQ, DQTBL$MS1_FRQ )
            MS2_FRQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT('", col,"') FROM ",schema,NAM_Repo," WHERE TO_CHAR(",col,") IN ('+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI')")))
            self.DQTBL.MS2_FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, MS2_FRQ, DQTBL$MS2_FRQ )
        
        else:
            MS1_FRQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT('", col,"') FROM ",schema,NAM_Repo," WHERE '", col, "' IS NULL OR CAST(", col, " AS VARCHAR) IN ('')")))
            self.DQTBL.MS1_FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, MS1_FRQ, DQTBL$MS1_FRQ )
            MS2_FRQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT('", col,"') FROM ",schema,NAM_Repo," WHERE CAST(", col, " AS VARCHAR) IN ('+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI')")))
            self.DQTBL.MS2_FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, MS2_FRQ, DQTBL$MS2_FRQ )