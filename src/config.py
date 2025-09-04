# config.py
# Centralized configuration for Risk Grouper Refactor

DEFAULT_CONFIG = {
    "SEED": 42,
    "EPSILON": 1e-6,
    "TOP_N": 25,
    "EXPLAIN_TRAIN_SAMPLE_SIZE": 500,
    "DATE_START": "2024-07-01",
    "DATE_END": "2024-12-01",
    "MODEL": "both",
    "INCLUDE_SPLITS": "train_val_test",
    "TARGET": "ANY_EVENT_NEXT_90D",  # <-- Set your target column name here
    "EXCLUDE_FEATURES": [

        #   "Enrollment status": [
        ##### - ALWAYS IGNORED VARIABLES #####
        
        'FH_ID','EFFECTIVE_MONTH_START', # <- always remove these, non-numeric data
        
        "age_bucket", #,- removed because redundant with the above
        
        "TOTAL_CONTACT_ATTEMPTS", "unsuccessful_contact_attempts", # - redundant with total contact attempts and success rate
        "new_missed_calls","increase_in_missed_calls",


        "SELF_PRESENT_THIS_MONTH",
        "med_adherence_present_this_month",
        "health_present_this_month", 
        "care_engagement_present_this_month",
        "program_trust_present_this_month", 
        "risk_harm_present_this_month", 
        "social_stability_present_this_month",        

    

        "DIABETES_TREATMENT_REQUIRED", "MH_TREATMENT_REQUIRED","CARDIO_TREATMENT_REQUIRED", "PULMONARY_TREATMENT_REQUIRED",
        "KIDNEY_TREATMENT_REQUIRED", "SUD_TREATMENT_REQUIRED","OTHER_COMPLEX_TREATMENT_REQUIRED",  # these are redundant with the diagnosis variables

        "ED_EVENT_NEXT_30D", "ED_EVENT_NEXT_60D", "ED_EVENT_NEXT_90D", # these are target variables
        "IP_EVENT_NEXT_30D", "IP_EVENT_NEXT_60D", "IP_EVENT_NEXT_90D", # these are target variables
        "IS_ACTIVE_NEXT_90D", "ANY_EVENT_NEXT_90D", # these are target variables

      
        "INSULIN_MPR_LAST_3_MONTHS", "ORAL_ANTIDIABETIC_MPR_LAST_3_MONTHS",
        "BETA_BLOCKER_MPR_LAST_3_MONTHS", "OPIATE_MPR_LAST_3_MONTHS",
        "ANTIPSYCH_MPR_LAST_3_MONTHS", 


        ##### - IGNORE UNLESS IT INCLUDES ATTRIBUTION LEVEL RUN #####
        "IS_EVER_SELECTED",  "IS_EVER_ENGAGED", #- include only when non-engaged data is used

        ##### - Horizon 3 METRICS  (COMMENT WHEN RUNNING) ###### IF WE REDUCE ALL LAG
        #"IS_ENROLLED", "IS_REINSTATED", "IS_DISENROLLED", 
        #"FIRST_DIAGNOSIS_DIABETES", "FIRST_DIAGNOSIS_MENTAL_HEALTH",
        #"FIRST_DIAGNOSIS_CARDIOVASCULAR", "FIRST_DIAGNOSIS_PULMONARY",
        #"FIRST_DIAGNOSIS_KIDNEY", "FIRST_DIAGNOSIS_SUD",
        #"FIRST_DIAGNOSIS_OTHER_COMPLEX",

        #"DIABETES_TREATED_THIS_MONTH", "MH_TREATED_THIS_MONTH", "CARDIO_TREATED_THIS_MONTH",
        #"PULMONARY_TREATED_THIS_MONTH","KIDNEY_TREATED_THIS_MONTH","SUD_TREATED_THIS_MONTH",
        #"OTHER_COMPLEX_TREATED_THIS_MONTH",

        #"DIABETES_NEW_PROVIDER_THIS_MONTH", "MH_NEW_PROVIDER_THIS_MONTH",
        #"CARDIO_NEW_PROVIDER_THIS_MONTH","PULMONARY_NEW_PROVIDER_THIS_MONTH",
        #"KIDNEY_NEW_PROVIDER_THIS_MONTH","SUD_NEW_PROVIDER_THIS_MONTH",
        #"OTHER_COMPLEX_NEW_PROVIDER_THIS_MONTH",

        #"INSULIN_MISSED_REFILL_THIS_MONTH", "ORAL_ANTIDIABETIC_MISSED_REFILL_THIS_MONTH",
        #"BETA_BLOCKER_MISSED_REFILL_THIS_MONTH", "OPIATE_MISSED_REFILL_THIS_MONTH",
        #"ANTIPSYCH_MISSED_REFILL_THIS_MONTH", 
        
        #"INSULIN_NEW_DRUG_THIS_MONTH",
        #"ORAL_ANTIDIABETIC_NEW_DRUG_THIS_MONTH", "BETA_BLOCKER_NEW_DRUG_THIS_MONTH",
        #"OPIATE_NEW_DRUG_THIS_MONTH", "ANTIPSYCH_NEW_DRUG_THIS_MONTH",

        
        #"SNF_THIS_MONTH", "ED_EVENTS_IN_MONTH", "IP_EVENTS_IN_MONTH",
        #"ED_EVENTS_LAST_3_MONTHS","IP_EVENTS_LAST_3_MONTHS","LAB_LAST_3_MONTHS", "SNF_LAST_3_MONTHS",

        #"DIABETES_TREATED_LAST_3_MONTHS", "MH_TREATED_LAST_3_MONTHS","CARDIO_TREATED_LAST_3_MONTHS",
        #"PULMONARY_TREATED_LAST_3_MONTHS","KIDNEY_TREATED_LAST_3_MONTHS","SUD_TREATED_LAST_3_MONTHS",
        #"OTHER_COMPLEX_TREATED_LAST_3_MONTHS",

        #"DIABETES_COORDINATED_CARE_LAST_3_MONTHS", "MH_COORDINATED_CARE_LAST_3_MONTHS",
        #"CARDIO_COORDINATED_CARE_LAST_3_MONTHS", "PULMONARY_COORDINATED_CARE_LAST_3_MONTHS", 
        #"KIDNEY_COORDINATED_CARE_LAST_3_MONTHS", "SUD_COORDINATED_CARE_LAST_3_MONTHS", 
        #"OTHER_COMPLEX_COORDINATED_CARE_LAST_3_MONTHS",

        #"NUM_INSULIN_DRUGS_LAST_3_MONTHS", "NUM_ORAL_ANTIDIABETIC_DRUGS_LAST_3_MONTHS", 
        #"NUM_BETA_BLOCKER_DRUGS_LAST_3_MONTHS","NUM_OPIATE_DRUGS_LAST_3_MONTHS", 
        #"NUM_ANTIPSYCH_DRUGS_LAST_3_MONTHS",
        
 
        ##### - Horizon 2 METRICS  (COMMENT WHEN RUNNING) ###### IF WE INCORPORATE OPERATIONAL VARIABLES
        
        #"success_rate_this_month","successful_contact_attempts",
        #"is_tried_contact_in_month", "is_succefful_contact_in_month",
        #"is_intense_attempt_in_month", "is_intense_support_in_month",
        #"became_non_responsive", "became_responsive",
        #"started_texting", "stopped_texting",
        #"started_intense_texting", "stopped_intense_texting",
        #"success_rate_increased", "success_rate_decreased",
     

        #"HAD_ED_EVENT_THIS_MONTH", "HAD_IP_EVENT_THIS_MONTH",
        #"HAD_ED_EVENT_LAST_3_MONTHS", "HAD_IP_EVENT_LAST_3_MONTHS",
        #"HAD_DETERIORATING_CONDITION_THIS_MONTH", "HAD_MEDICATION_CONCERN_THIS_MONTH",
        #"HAD_MEDICAL_NEEDS_THIS_MONTH", "NEEDED_TRIAGE_ESCALATION_THIS_MONTH",
        #"TRIAGE_ESCALATION_RESOLVED_THIS_MONTH", "TRIAGE_ESCALATION_UNRESOLVED_THIS_MONTH",       

        #"self_score_high_this_month","med_adherence_score_high_this_month","health_score_high_this_month",
        #"care_engagement_score_high_this_month","program_trust_score_high_this_month","risk_harm_score_high_this_month",
        #"social_stability_score_high_this_month",

        

        ##### - Horizon 1 METRICS  (COMMENT WHEN RUNNING) ###### IF WE ONLY USE CLAIMS BASED VARIABLES

       # "HAS_DIABETES", "HAS_MENTAL_HEALTH", "HAS_CARDIOVASCULAR","HAS_PULMONARY", "HAS_KIDNEY", "HAS_SUD", "HAS_OTHER_COMPLEX",

        #"DIABETES_EVER_TREATED","MH_EVER_TREATED","CARDIO_EVER_TREATED", "PULMONARY_EVER_TREATED","KIDNEY_EVER_TREATED","SUD_EVER_TREATED","OTHER_COMPLEX_EVER_TREATED",
       
        #"INSULIN_EVER_PRESCRIBED", "ORAL_ANTIDIABETIC_EVER_PRESCRIBED", "BETA_BLOCKER_EVER_PRESCRIBED", "OPIATE_EVER_PRESCRIBED", "ANTIPSYCH_EVER_PRESCRIBED",

        #"INSULIN_MED_ADHERENT", "ORAL_ANTIDIABETIC_MED_ADHERENT", "BETA_BLOCKER_MED_ADHERENT", "OPIATE_MED_ADHERENT", "ANTIPSYCH_MED_ADHERENT",
       
        #"LAB_LAST_6_MONTHS", "LAB_LAST_12_MONTHS",

        #"is_habitual_ed_user", "is_episodic_ed_user", "is_sporadic_ed_user",

        #"year","is_jan","is_feb","is_mar","is_apr","is_may","is_jun","is_jul","is_aug","is_sep","is_oct","is_nov","is_dec",

        #"is_selected_month", "is_engaged_month", "months_since_engagement",

        #"IS_ABD", "IS_TANF", "IS_EXPANSION", "IS_DSNP", "IS_OTHER", 

        #"is_male", "is_female", "is_gender_unknown", "is_age_15_19",
        #"is_age_20_24", "is_age_25_29", "is_age_30_34", "is_age_35_39","is_age_40_44", "is_age_45_49", "is_age_50_54", "is_age_55_59",
        #"is_age_60_64", "is_age_65_69", "is_age_70_74", "is_age_75_79","is_age_80_84", "is_age_85_89", "is_age_90_94", "is_age_95_99",
        #"is_age_100_plus",

        #"is_market_canton", "is_market_youngstown", "is_market_memphis", "is_market_dayton", 
        #"is_market_richmond", "is_market_cleveland", "is_market_chattanooga", "is_market_detROIT", 
        #"is_market_orlando", "is_market_toledo", "is_market_upper_east_tn", "is_market_akron", 
        #"is_market_columbus", "is_market_nashville", "is_market_jacksonville", "is_market_cincinnati", 
        #"is_market_fairfax", "is_market_support", "is_market_tacoma", "is_market_miami", 
        #"is_market_knoxville", "is_market_southwest_virginia", "is_market_other",

 
        ##### - HORIZON 0 METRICS (COMMENT WHEN RUNNING) 
  
        #"ED_EVENTS_LAST_3_6_MONTHS", "IP_EVENTS_LAST_3_6_MONTHS",        
        #IS_HIGH_RISK

    ]
}

# CLI overrides will update DEFAULT_CONFIG at runtime via argparse in updated_seperate_regression_models.py