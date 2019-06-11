# Demographic module (OMOP):
# Race, gender, ethnicity, percentage of pop., organization site

# Use from indicators
import pandas

class Demographic:
    def __init__(self, query: object):
        self.query = query

    def get(self):
        if self.query.CDM.startswith("OMOP"):
            male = 8507
            female = 8532

            hispanic_latino = 38003563
            not_hispanic_latino = 38003564

            race_dict = {
                "American_Indian" : 38003572,
                "Alaska_Native" : 38003573,
                "Asian_Indian" : 38003574,
                "Bangladeshi" : 38003575,
                "Bhutanese" : 38003576,
                "Burmese" : 38003577,
                "Cambodian" : 38003578,
                "Chinese" : 38003579,
                "Taiwanese" : 38003580,
                "Filipino" : 38003581,
                "Hmong" : 38003582,
                "Indonesian" : 38003583,
                "Japanese" : 38003584,
                "Korean" : 38003585,
                "Laotian" : 38003586,
                "Malaysian" : 38003587,
                "Okinawan" : 38003588,
                "Pakistani" : 38003589,
                "Sri_Lankan" : 38003590,
                "Thai" : 38003591,
                "Vietnamese" : 38003592,
                "Iwo Jiman" : 38003593,
                "Maldivian" : 38003594,
                "Nepalese" : 38003595,
                "Singaporean" : 38003596,
                "Madagascar" : 38003597,
                "Black" : 38003598,
                "African_American" : 38003599,
                "African" : 38003600,
                "Bahamian" : 38003601,
                "Barbadian" : 38003602,
                "Dominican" : 38003603,
                "Dominica_Islander" : 38003604,
                "Haitian": 38003605,
                "Jamaican" : 38003606,
                "Tobagoan" : 38003607,
                "Trinidadian" : 38003608,
                "West_Indian" : 38003609,
                "Polynesian" : 38003610,
                "Micronesian" : 38003611,
                "Melanesian" : 38003612,
                "Other_Pacific_Islander" : 38003613,
                "European" : 38003614,
                "Middle_Eastern_or_North_African" : 38003615,
                "Arab" : 38003616,
                "Asian" : 8515,
                "Black_or_African_American" : 8516,
                "White" : 8527,
                "Native_Hawaiian_or_Other_Pacific_Islander" : 8557,
                "American_Indian_or_Alaska_Native" : 8657
            }

            gender_df = self.gender(id="person_id",
                                    table="PERSON",
                                    col="gender_concept_id",
                                    male=male,
                                    female=female)
            ethnicity_df = self.ethnicity(id="person_id",
                                          table="PERSON",
                                          col="ethnicity_concept_id",
                                          hispanic_latino=hispanic_latino,
                                          not_hispanic_latino=not_hispanic_latino)
            race_df = self.race(id="person_id",
                                table="PERSON",
                                col="race_concept_id",
                                race_dict=race_dict)
        
        elif self.query.CDM.startswith("PCORNET"):
            male = "M"
            female = "F"

            hispanic_latino = "Y"
            not_hispanic_latino = "N"

            race_dict = {
                "American_Indian_or_Alaska_Native": "01",
                "Asian" : "02",
                "Black_or_African_American" : "03",
                "Native_Hawaiian_or_Other_Pacific_Islander" : "04",
                "White" : "05",
                "Multiple_Race" : "06",
                "Refuse_to_answer" : "07",
                "Other" : "OT"
            }
            
            gender_df = self.gender(id="PATID",
                                    table="DEMOGRAPHIC",
                                    col="sex",
                                    male=male,
                                    female=female)
            ethnicity_df = self.ethnicity(id="PATID",
                                          table="DEMOGRAPHIC",
                                          col="hispanic",
                                          hispanic_latino=hispanic_latino,
                                          not_hispanic_latino=not_hispanic_latino)
            race_df = self.race(id="PATID",
                                table="DEMOGRAPHIC",
                                col="race",
                                race_dict=race_dict)
        
        else:
            raise Exception(f"No config.json file for CDM {self.query.CDM}")

        return # csv file demographic.csv

    def gender(self, id: str, table: str, col: str, male=None, female=None) -> object:
        male_query: str = f"""
                            SELECT COUNT(DISTINCT({id})) as male_count
                            FROM {self.query.prefix}{table}
                            WHERE {col} == {male} """
        
        female_query: str = f"""
                            SELECT COUNT(DISTINCT({id})) as female_count
                            FROM {self.query.prefix}{table}
                            WHERE {col} == {female} """
        
        return pandas.DataFrame({
            "Male": [pandas.read_sql(male_query, con=self.query.conn)["male_count"]],
            "Female": [pandas.read_sql(female_query, con=self.query.conn)["female_count"]]
        })
    
    def ethnicity(self, id: str, table: str, col: str, hispanic_latino=None, not_hispanic_latino=None) -> object:
        hispanic_latino_query: str = f"""
                            SELECT COUNT(DISTINCT({id})) as hispanic_latino_count
                            FROM {self.query.prefix}{table}
                            WHERE {col} == {hispanic_latino} """
        
        not_hispanic_latino_query: str = f"""
                            SELECT COUNT(DISTINCT({id})) as not_hispanic_latino_count
                            FROM {self.query.prefix}{table}
                            WHERE {col} == {not_hispanic_latino} """
        
        return pandas.DataFrame({
            "Hispanic_or_Latino": [pandas.read_sql(hispanic_latino_query, con=self.query.conn)["hispanic_latino_count"]],
            "Not_Hispanic_or_Latino": [pandas.read_sql(not_hispanic_latino_query, con=self.query.conn)["not_hispanic_latino_count"]]
        })

    def race(self, id: str, table: str, col: str, race_dict=None) -> object:
        race_df = pandas.DataFrame()

        for key in race_dict:
            race_query: str = f"""
                                SELECT COUNT(DISTINCT({id})) as race_count
                                FROM {self.query.prefix}{table}
                                WHERE {col} == {race_dict[key]} """
            race_df[key] = [pandas.read_sql(race_query, con=self.query.conn)["race_count"]]
        
        return race_df