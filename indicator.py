from prep import Prep
import pandas

class Indicator:
    def __init__(self, query: object):
        self.query = query

    def get(self):
        if Prep().CDM == "PCORNET3" or Prep().CDM == "PCORNET31":
            withoutGenderPCORI      = self.query.withoutdemPCORI(col = "sex", group = "Gender")
            withoutRacePCORI        = self.query.withoutdemPCORI(col = "race", group = "Race")
            withoutEthnicityPCORI   = self.query.withoutdemPCORI(col = "hispanic", group = "Ethnicity")

            withoutMedicationPCORI  = self.query.withoutPCORI(table = "PRESCRIBING", col = "prescribingid", group = "Medication")
            withoutDiagnosisPCORI   = self.query.withoutPCORI(table = "DIAGNOSIS", col = "dx", group = "Diagnosis")
            withoutEncounterPCORI   = self.query.withoutPCORI(table = "ENCOUNTER", col = "enc_type", group = "Encounter")
            withoutWeightPCORI      = self.query.withoutPCORI(table = "VITAL", col = "wt", group = "Weight")
            withoutHeightPCORI      = self.query.withoutPCORI(table = "VITAL", col = "ht", group = "Height")
            withoutBpSysPCORI       = self.query.withoutPCORI(table = "VITAL", col = "systolic", group = "BP")
            withoutBpDiasPCORI      = self.query.withoutPCORI(table = "VITAL", col = "diastolic", group = "BP")
            withoutSmokingPCORI     = self.query.withoutPCORI(table = "VITAL", col = "smoking", group = "Smoking")

            indicators = pandas.concat([withoutGenderPCORI, withoutRacePCORI, withoutEthnicityPCORI, withoutMedicationPCORI,
                                    withoutDiagnosisPCORI, withoutEncounterPCORI, withoutWeightPCORI, withoutHeightPCORI,
                                    withoutBpSysPCORI, withoutBpDiasPCORI, withoutSmokingPCORI], ignore_index=True)

        elif Prep().CDM == "OMOPV5_0" or Prep().CDM == "OMOPV5_2" or Prep().CDM == "OMOPV5_3":
            withoutGenderOMOP       = self.query.withoutdemOMOP(col = "gender_concept_id", group = "Gender")
            withoutRaceOMOP         = self.query.withoutdemOMOP(col = "race_concept_id", group = "Race")
            withoutEthnicityOMOP    = self.query.withoutdemOMOP(col = "ethnicity_concept_id", group = "Ethnicity")

            withoutBpOMOP           = self.query.isPresentOMOP(table = "MEASUREMENT", col = "measurement_concept_id", group = "BP")
            withoutHrOMOP           = self.query.isPresentOMOP(table = "MEASUREMENT", col = "measurement_concept_id", group = "HR")
            withoutHeightOMOP       = self.query.isPresentOMOP(table = "MEASUREMENT", col = "measurement_concept_id", group = "Height")
            withoutWeightOMOP       = self.query.isPresentOMOP(table = "MEASUREMENT", col = "measurement_concept_id", group = "Weight")
            withoutSmokingOMOP      = self.query.isPresentOMOP(table = "OBSERVATION", col = "observation_concept_id", group = "Smoker")

            withoutMedicationOMOP   = self.query.withoutOMOP(table = "DRUG_EXPOSURE", col = "drug_exposure_id", group = "Medication")
            withoutDiagnosisOMOP    = self.query.withoutOMOP(table = "CONDITION_OCCURRENCE", col = "condition_occurrence_id", group = "Condition")
            withoutVisitOMOP        = self.query.withoutOMOP(table = "VISIT_OCCURRENCE", col = "visit_occurrence_id", group = "Visit")

            indicators = pandas.concat([withoutGenderOMOP, withoutRaceOMOP, withoutEthnicityOMOP, withoutBpOMOP, withoutHrOMOP,
                                    withoutHeightOMOP, withoutWeightOMOP, withoutSmokingOMOP, withoutMedicationOMOP,
                                    withoutDiagnosisOMOP, withoutVisitOMOP], ignore_index=True)

        return indicators.to_csv("reports/indicators.csv")
