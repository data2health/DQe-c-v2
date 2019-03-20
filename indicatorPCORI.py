# CDM: PCORI
# DBMS: SQL SERVER OR ORACLE, DOES NOT INCLUDE REDSHIFT OR POSTGRESQL

without_gender      = withoutdemPCORI(col = "sex", group="Gender")
without_race        = withoutdemPCORI(col = "race", group="Race")
without_ethnicity   = withoutdemPCORI(col = "hispanic", group="Ethnicity")

without_medication  = withoutPCORI(table = "PRESCRIBING", col = "prescribingid",group="Medication")
without_diagnosis   = withoutPCORI(table = "DIAGNOSIS", col = "dx",group="Diagnosis")
without_encounter   = withoutPCORI(table = "ENCOUNTER", col = "enc_type",group="Encounter")
without_weight      = withoutPCORI(table = "VITAL", col = "wt",group="Weight")
without_height      = withoutPCORI(table = "VITAL", col = "ht",group="Height")
without_BP_sys      = withoutPCORI(table = "VITAL", col = "systolic",group="BP")
without_BP_dias     = withoutPCORI(table = "VITAL", col = "diastolic",group="BP")
without_smoking     = withoutPCORI(table = "VITAL", col = "smoking",group="Smoking")

################## FUNCTIONS ######################
def withoutdemPCORI(col,group):
    exclude = {
            "Gender": ["M","F"],
            "Race": ["05","03","07","02","01","04","06","OT"],
            "Ethnicity": ["Y"]
           }
    denominatorQuery: str = f"""
                            SELECT COUNT(DISTINCT(PATID))
                            FROM DEMOGRAPHIC
                            WHERE BIRTH_DATE > 1900-01-01 AND BIRTH_DATE  < 2014-01-01 """
    notinQuery: str = f"""
                      SELECT COUNT(PATID)
                      FROM  (
                            SELECT *
                            FROM {self.schema}{self.prefix}DEMOGRAPHIC
                            WHERE BIRTH_DATE > 1900-01-01 AND BIRTH_DATE  < 2014-01-01
                            ) dd
                      WHERE toupper({col}) NOT IN {exclude[group]} """
    whattheyhaveQuery: str = f"""
                             SELECT DISTINCT(toupper({col}))
                             FROM   (
                                    SELECT *
                                    FROM {self.schema}{self.prefix}DEMOGRAPHIC
                                    WHERE BIRTH_DATE > 1900-01-01 AND BIRTH_DATE  < 2014-01-01
                                    ) dd
                            WHERE toupper({col}) NOT IN {exclude[group]} """
    
    cursor = self.conn.cursor()
    denominator = cursor.execute(denominatorQuery)
    notin = cursor.execute(notinQuery)
    whattheyhave = cursor.execute(whattheyhaveQuery)
    self.conn.close()

    d1 = round((notin/denominator)*100,4)
    # MESSAGES
    # print("%s of patients born between 1900-01-01 and 2014-01-01 are missing %s information.", d1, list)
    # if (d1 > 0):
    #     print("%s of the %s patients born between 1900-01-01 and 2014-01-01 don't have an acceptable %s record in the %s table.", notin, denominator, toupper(list), toupper(df))
    #     print("Unacceptable values in column %s are %s.", toupper(col), whattheyhave)
    return pandas.DataFrame({
                            "GROUP": [group],
                            "MISSING PERCENTAGE": [d1],
                            "MISSING POPULATION": [notin],
                            "DENOMINATOR": [denominator],
                            "PERCENTAGE": [str(round(d1,2))+"%"],
                            "TEST_DATE": [datetime.datetime.today().strftime('%m-%d-%Y')],
                            "ORGANIZATION": [self.organization],
                            "CDM": [self.CDM]
                            })

# this list here works opposite to the list in the function above. here we identify what we don't want.
def withoutPCORI(table,col,group):
    exclude: list       = ["","%","$","#","@","NI"]
    denominatorQuery: str = f"""
                            SELECT COUNT(DISTINCT(PATID))
                            FROM {self.schema}{self.prefix}DEMOGRAPHIC
                            WHERE BIRTH_DATE > 1900-01-01 AND BIRTH_DATE  < 2014-01-01 """


    if DBMS == "sql server":
        pats_wit_oneoutQuery = f"""
                               SELECT COUNT(DISTINCT(PATID))
                               FROM {self.schema}{table}
                               WHERE toupper({col}) IS NULL OR CAST(toupper({col}) AS CHAR(54)) IN  {exclude} """
        patsWithoutRecordsQuery = f"""
                                  SELECT COUNT(DISTINCT(PATID))
                                  FROM {self.schema}{self.prefix}DEMOGRAPHIC
                                  WHERE BIRTH_DATE > 1900-01-01
                                    AND BIRTH_DATE  < 2014-01-01
                                    AND PATID IN  (
                                                  SELECT DISTINCT(PATID)
                                                  FROM {self.schema}{table}
                                                  WHERE toupper({col}) IS NOT NULL AND CAST(toupper({col}) AS CHAR(54)) NOT IN {exclude}
                                                  ) """

    elif DBMS == "oracle":
        pats_wit_oneoutQuery = f"""
                               SELECT COUNT(DISTINCT(PATID))
                               FROM {self.schema}{table}
                               WHERE toupper({col}) IS NULL OR TO_CHAR(toupper({col})) IN {exclude} """
        
        patsWithoutRecordsQuery = f"""
                                  SELECT COUNT(DISTINCT(PATID))
                                  FROM {self.schema}{self.prefix}DEMOGRAPHIC
                                  WHERE BIRTH_DATE > 1900-01-01
                                    AND BIRTH_DATE  < 2014-01-01
                                    AND PATID IN  (
                                                  SELECT DISTINCT(PATID)
                                                  FROM {self.schema}{table}
                                                  WHERE toupper({col}) IS NOT NULL AND TO_CHAR(toupper({col})) NOT IN {exclude}
                                                  ) """

    cursor = self.conn.cursor()
    denominator = cursor.execute(denominatorQuery)
    pats_wit_oneout = cursor.execute(pats_wit_oneoutQuery)
    patsWithoutRecords = cursor.execute(patsWithoutRecordsQuery)
    self.conn.close()

    ppwo = round((pats_wit_oneout/denominator)*100,4)
    pwse = round(((denominator-patsWithoutRecords)/denominator)*100,4)
    # if (ppwo > 1) message(pats_wit_oneout, " of the patients -- ",ppwo,"% of patients -- are missing at least 1 acceptable ",toupper(col)," value in the ",toupper(table)," table.",appendLF=T)
    # if (pwse > 1) message(patsWithoutRecords, " of the patients -- ",pwse,"% of patients -- are missing any acceptable ",toupper(col)," value in the ",toupper(table)," table.",appendLF=T)
    # message(pwse, "% of unique patients don't have any '", list.name,"' record in the ",df.name, " table.",appendLF=T)
    return pandas.DataFrame({
                            "GROUP": [group],
                            "MISSING PERCENTAGE": [pwse],
                            "MISSING POPULATION": [patsWithoutRecords],
                            "DENOMINATOR": [denominator],
                            "PERCENTAGE": [str(round(pwse,2))+"%"],
                            "TEST_DATE": [datetime.datetime.today().strftime('%m-%d-%Y')],
                            "ORGANIZATION": [self.organization],
                            "CDM": [self.CDM]
                            })