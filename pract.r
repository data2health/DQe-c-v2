# add columns needed for completeness analysis

DQTBL$test_date <- as.factor(test_date)
DQTBL$FRQ <- 0
DQTBL$UNIQFRQ <- 0
DQTBL$MS1_FRQ <- 0 # for NULL/NAs
DQTBL$MS2_FRQ <- 0 # for ""s
DQTBL$MSs_PERC<- 0 # for percent missingness
DQTBL$organization <- org #ORGANIZATION NAME
DQTBL$test_date <- as.character(format(Sys.Date(),"%m-%d-%Y"))
DQTBL$CDM <- CDM # Data Model



##store a table with list of all tables and columns in the repository

if (SQL == "SQLServer") {repotabs <- dbGetQuery(conn,"SELECT COLUMN_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS") 
} else if (SQL == "Oracle") {repotabs <- dbGetQuery(conn,"SELECT COLUMN_NAME, TABLE_NAME FROM user_tab_cols")
} else if (SQL == "PostgreSQL") {repotabs <- dbGetQuery(conn,"SELECT COLUMN_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS")
} else if (SQL == "Redshift") {repotabs <- dbGetQuery(conn, paste("SELECT COLUMN_NAME, TABLE_NAME 
                                                                  FROM INFORMATION_SCHEMA.COLUMNS
                                                                  WHERE table_schema='",schema_orig,"';", sep=""))
}

colnames(repotabs)[1] <- toupper(colnames(repotabs)[1])
colnames(repotabs)[2] <- toupper(colnames(repotabs)[2])


#############################################################################
##loop 1: go through all columns in all tables and count number of rows 
##Results will be stored in column "FRQ" of the DQTBL table
#############################################################################

for (j in 1: length(unique(DQTBL$TabNam))) 
  ##DQTBL$TabNam has all table names
{
  NAM <-  unique(DQTBL$TabNam)[j]
  
  
  ##extracted name of table j in CDM
  NAM_Repo <- as.character(tbls2[(tbls2$CDM_Tables == NAM),"Repo_Tables"])
  
###############* what's the point of checking this if condition? *###################
  if (!identical(NAM_Repo, character(0))) { 
    # L <- as.numeric(tbls2[(tbls2$CDM_Tables == NAM),"NCols"])
    id.NAM <- which(DQTBL$TabNam == NAM)
  
    id.repotabs <- which(repotabs$TABLE_NAME == NAM_Repo)
    ##extracting the row numbers
    NAMTB <- DQTBL[id.NAM,]
    REPOTB <- repotabs[id.repotabs,]
    
    ##subsetting the DQTBL and repository table to only the rows from table j
    ##saving the name of table j as characters
    
    for (i in 1:dim(REPOTB)[1])
      ##now going through the columns of table j
    {
      col <- REPOTB$COLUMN_NAME[i]
      FRQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT(*) FROM ",schema,NAM_Repo)))
      ##calculated length (number of total rows) of each column from each table
      DQTBL$FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, FRQ, DQTBL$FRQ )
      ##stored frequency in the culumn FRQ
      UNIQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT(DISTINCT ", col,") FROM ",schema,NAM_Repo)))
      ##calculated length (number of total rows) of each column from each table
      DQTBL$UNIQFRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, UNIQ, DQTBL$UNIQFRQ )
      ##stored frequency in the culumn FRQ
    }
  }
}

###########################################
############ COMPLETENESS ANALYSIS ########
###########################################
# This scripts counts and stores frequency of missing values

  for (j in 1: length(unique(DQTBL$TabNam))) 
    ##DQTBL$TabNam has all table names
  {
    NAM <-  unique(DQTBL$TabNam)[j]
    ##extracted name of table j in CDM
    NAM_Repo <- as.character(tbls2[(tbls2$CDM_Tables == NAM),"Repo_Tables"])
    # L <- as.numeric(tbls2[(tbls2$CDM_Tables == NAM),"NCols"])
    id.NAM <- which(DQTBL$TabNam == NAM)
    id.repotabs <- which(repotabs$TABLE_NAME == NAM_Repo)
    ##extracting the row numbers
    NAMTB <- DQTBL[id.NAM,]
    REPOTB <- repotabs[id.repotabs,]
    ##subsetting the DQTBL and repository table to only the rows from table j
    ##saving the name of table j as characters
    
    for (i in 1:dim(REPOTB)[1])
      ##now going through the columns of table j
    {
      col <- REPOTB$COLUMN_NAME[i]
      SQLServer_Redshift_MS1_FRQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT('", col,"') FROM ",schema,NAM_Repo," WHERE [", col, "] IS NULL OR CAST(", col, " AS VARCHAR) IN ('')")))
      ##calculated length (number of total rows) of each column from each table
      DQTBL$SQLServer_Redshift_MS1_FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, MS1_FRQ, DQTBL$MS1_FRQ )
      ##stored frequency in the culumn FRQ
      SQLServer_Redshift_MS2_FRQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT('", col,"') FROM ",schema,NAM_Repo," WHERE CAST(", col, " AS VARCHAR) IN ('+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI')")))
      ##calculated length (number of total rows) of each column from each table
      DQTBL$SQLServer_Redshift_MS2_FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, MS2_FRQ, DQTBL$MS2_FRQ )
      ##stored frequency in the culumn FRQ
      Oracle_MS1_FRQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT('", col,"') FROM ",schema,NAM_Repo," WHERE ", col, " IS NULL OR TO_CHAR(", col, ") IN ('')")))
      ##calculated length (number of total rows) of each column from each table
      DQTBL$Oracle_MS1_FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, MS1_FRQ, DQTBL$MS1_FRQ )
      ##stored frequency in the culumn FRQ
      Oracle_MS2_FRQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT('", col,"') FROM ",schema,NAM_Repo," WHERE TO_CHAR(",col,") IN ('+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI')")))
      ##calculated length (number of total rows) of each column from each table
      DQTBL$Oracle_MS2_FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, MS2_FRQ, DQTBL$MS2_FRQ )
      ##stored frequency in the culumn FRQ
      PostgreSQL_MS1_FRQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT('", col,"') FROM ",schema,NAM_Repo," WHERE '", col, "' IS NULL OR CAST(", col, " AS VARCHAR) IN ('')")))
      ##calculated length (number of total rows) of each column from each table
      DQTBL$PostgreSQL_MS1_FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, MS1_FRQ, DQTBL$MS1_FRQ )
      ##stored frequency in the culumn FRQ
      PostgreSQL_MS2_FRQ <- as.numeric(dbGetQuery(conn, paste0("SELECT COUNT('", col,"') FROM ",schema,NAM_Repo," WHERE CAST(", col, " AS VARCHAR) IN ('+', '-', '_','#', '$', '*', '\', '?', '.', '&', '^', '%', '!', '@','NI')")))
      ##calculated length (number of total rows) of each column from each table
      DQTBL$PostgreSQL_MS2_FRQ <- ifelse(DQTBL$ColNam == tolower(col) & DQTBL$TabNam == NAM, MS2_FRQ, DQTBL$MS2_FRQ )
      ##stored frequency in the culumn FRQ
    }
  }
      

DQTBL$FRQ <- as.numeric(DQTBL$FRQ)
DQTBL$MS1_FRQ <- as.numeric(DQTBL$MS1_FRQ)
DQTBL$MS2_FRQ <- as.numeric(DQTBL$MS2_FRQ)

##calculating percent missing compared to the entire rows in each column/table
DQTBL$MSs_PERC <- round((DQTBL$MS1_FRQ+DQTBL$MS2_FRQ)/DQTBL$FRQ,2)
##saving the master DQ table
write.csv(DQTBL, file = paste("reports/mstabs/DQ_Master_Table_",CDM,"_",org,"_",as.character(format(Sys.Date(),"%d-%m-%Y")),".csv", sep=""))

##saving a copy for aggregated analysis, if the aggregated analysis add-on is installed.

#set the PATH below to aggregatted analysis directory
# write.csv(DQTBL, file = paste("PATH/DQ_Master_Table_",CDM,"_",org,"_",as.character(format(Sys.Date(),"%d-%m-%Y")),".csv", sep=""))





##### Creating FRQ_comp table to compare frequencies from MSDQ table over time.
path = "reports/mstabs"
msnames <- list.files(path)
n <- length(msnames)

##reading and storing master DQ tables
compr <- list()
N <- length(msnames)
for (n in 1:N) {
  compr[[n]] = data.frame(read.csv(paste0(path,"/",msnames[n],sep="")))
}

#binding the tables together to create a masters table
if (CDM %in% c("PCORNET3","PCORNET31")) {
  FRQ_comp <- subset(rbindlist(compr),
                       (ColNam == "patid" & TabNam == "demographic") |
                       (ColNam == "dispensingid" & TabNam == "dispensing") |
                       (ColNam == "vitalid" & TabNam == "vital") |
                       (ColNam == "conditionid" & TabNam == "condition") |
                       (ColNam == "pro_cm_id" & TabNam == "pro_cm") |
                       (ColNam == "encounterid" & TabNam == "encounter") |
                       (ColNam == "diagnosisid" & TabNam == "diagnosis") |
                       (ColNam == "proceduresid" & TabNam == "procedures") |
                       # (ColNam == "providerid" & TabNam == "encounter") |
                       (ColNam == "prescribingid" & TabNam == "prescribing") |
                       (ColNam == "trialid" & TabNam == "pcornet_trial") |
                       (ColNam == "networkid" & TabNam == "harvest") 
  )
} else if (CDM %in% c("OMOPV5_2","OMOPV5_3")) {
  FRQ_comp <- subset(rbindlist(compr,fill=TRUE),
                       (ColNam == "person_id" & TabNam == "person") |
                       (ColNam == "measurement_id" & TabNam == "measurement") |
                       (ColNam == "visit_occurrence_id" & TabNam == "visit_occurrence") |
                       (ColNam == "condition_occurrence_id" & TabNam == "condition_occurrence") |
                       (ColNam == "procedure_occurrence_id" & TabNam == "procedure_occurrence") |
                       (ColNam == "observation_id" & TabNam == "observation") |
                       (ColNam == "visit_detail_id" & TabNam == "visit_detail")
  )
} else if (CDM %in% c("OMOPV5_0")) {
  FRQ_comp <- subset(rbindlist(compr,fill = TRUE),
                       (ColNam == "person_id" & TabNam == "person") |
                       (ColNam == "measurement_id" & TabNam == "measurement") |
                       (ColNam == "visit_occurrence_id" & TabNam == "visit_occurrence") |
                       (ColNam == "condition_occurrence_id" & TabNam == "condition_occurrence") |
                       (ColNam == "procedure_occurrence_id" & TabNam == "procedure_occurrence") |
                       (ColNam == "observation_id" & TabNam == "observation")
  )
}

write.csv(FRQ_comp, file = paste("reports/FRQ_comp_",CDM,"_",org,"_",as.character(format(Sys.Date(),"%d-%m-%Y")),".csv", sep=""))