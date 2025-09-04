import pandas as pd
import os

# Path to cached claims dataset
claims_path = "cache/claims_only_dataset.parquet"

# Load the claims dataset
print(f"Loading claims data from {claims_path} ...")
df = pd.read_parquet(claims_path)
print(f"Loaded {len(df):,} rows.")

# Check for expected columns
print("Columns:", df.columns.tolist())


# Extract year from EVENT_DATE, coerce errors (invalid dates become NaT)
df['EVENT_DATE_parsed'] = pd.to_datetime(df['EVENT_DATE'], errors='coerce')
invalid_dates = df['EVENT_DATE_parsed'].isna().sum()
if invalid_dates > 0:
    print(f"Warning: {invalid_dates} rows have invalid EVENT_DATE and will be dropped.")
df = df.dropna(subset=['EVENT_DATE_parsed'])
df['year'] = df['EVENT_DATE_parsed'].dt.year


# Unique members per year (MEMBER_ID)
members_per_year = df.groupby('year')['MEMBER_ID'].nunique()
print("\nUnique members per year:")
print(members_per_year)

# Total rows per year
rows_per_year = df['year'].value_counts().sort_index()
print("\nTotal rows per year:")
print(rows_per_year)

# Save summary to CSV
summary = pd.DataFrame({
    'unique_members': members_per_year,
    'total_rows': rows_per_year,
    'avg_interactions_per_member': rows_per_year / members_per_year,
    # Correct event aggregation: sum events per member per year, then average across members
    'avg_ip_events': df.groupby(['year', 'MEMBER_ID'])[['CNT_IP_VISITS_90D', 'CNT_IP_VISITS_180D']].sum().sum(axis=1).groupby('year').mean(),
    'avg_ed_events': df.groupby(['year', 'MEMBER_ID'])[['CNT_ED_VISITS_90D', 'CNT_ED_VISITS_180D']].sum().sum(axis=1).groupby('year').mean(),
    'avg_paid_sum_90d': df.groupby('year')['PAID_SUM_90D'].sum() / members_per_year,
    'avg_paid_sum_180d': df.groupby('year')['PAID_SUM_180D'].sum() / members_per_year,
    'mean_health_score': df.groupby('year')['NOTE_HEALTH_SCORE'].mean(),
    'mean_risk_harm_score': df.groupby('year')['NOTE_RISK_HARM_SCORE'].mean(),
    'mean_social_stab_score': df.groupby('year')['NOTE_SOCIAL_STAB_SCORE'].mean(),
    'mean_med_adherence_score': df.groupby('year')['NOTE_MED_ADHERENCE_SCORE'].mean(),
    'mean_care_engagement_score': df.groupby('year')['NOTE_CARE_ENGAGEMENT_SCORE'].mean(),
    'mean_program_trust_score': df.groupby('year')['NOTE_PROGRAM_TRUST_SCORE'].mean(),
    'mean_self_score': df.groupby('year')['NOTE_SELF_SCORE'].mean(),
    'avg_hcc_diabetes': (df.groupby('year')['CNT_HCC_DIABETES_90D'].sum() + df.groupby('year')['CNT_HCC_DIABETES_180D'].sum()) / members_per_year,
    'avg_hcc_mental_health': (df.groupby('year')['CNT_HCC_MENTAL_HEALTH_90D'].sum() + df.groupby('year')['CNT_HCC_MENTAL_HEALTH_180D'].sum()) / members_per_year,
    'avg_hcc_cardiovascular': (df.groupby('year')['CNT_HCC_CARDIOVASCULAR_90D'].sum() + df.groupby('year')['CNT_HCC_CARDIOVASCULAR_180D'].sum()) / members_per_year,
    'avg_hcc_pulmonary': (df.groupby('year')['CNT_HCC_PULMONARY_90D'].sum() + df.groupby('year')['CNT_HCC_PULMONARY_180D'].sum()) / members_per_year,
    'avg_hcc_kidney': (df.groupby('year')['CNT_HCC_KIDNEY_90D'].sum() + df.groupby('year')['CNT_HCC_KIDNEY_180D'].sum()) / members_per_year,
    'avg_hcc_sud': (df.groupby('year')['CNT_HCC_SUD_90D'].sum() + df.groupby('year')['CNT_HCC_SUD_180D'].sum()) / members_per_year,
    'avg_any_hcc': (df.groupby('year')['CNT_ANY_HCC_90D'].sum() + df.groupby('year')['CNT_ANY_HCC_180D'].sum()) / members_per_year,
    'avg_psychotherapy': (df.groupby('year')['CNT_PROC_PSYCHOTHERAPY_90D'].sum() + df.groupby('year')['CNT_PROC_PSYCHOTHERAPY_180D'].sum()) / members_per_year,
    'avg_psychiatric_evals': (df.groupby('year')['CNT_PROC_PSYCHIATRIC_EVALS_90D'].sum() + df.groupby('year')['CNT_PROC_PSYCHIATRIC_EVALS_180D'].sum()) / members_per_year,
    'avg_fills_antipsychotic': (df.groupby('year')['CNT_FILLS_ANTIPSYCHOTIC_90D'].sum() + df.groupby('year')['CNT_FILLS_ANTIPSYCHOTIC_180D'].sum()) / members_per_year,
    'avg_fills_insulin': (df.groupby('year')['CNT_FILLS_INSULIN_90D'].sum() + df.groupby('year')['CNT_FILLS_INSULIN_180D'].sum()) / members_per_year,
    'avg_fills_oral_antidiab': (df.groupby('year')['CNT_FILLS_ORAL_ANTIDIAB_90D'].sum() + df.groupby('year')['CNT_FILLS_ORAL_ANTIDIAB_180D'].sum()) / members_per_year,
    'avg_fills_statin': (df.groupby('year')['CNT_FILLS_STATIN_90D'].sum() + df.groupby('year')['CNT_FILLS_STATIN_180D'].sum()) / members_per_year,
    'avg_fills_beta_blocker': (df.groupby('year')['CNT_FILLS_BETA_BLOCKER_90D'].sum() + df.groupby('year')['CNT_FILLS_BETA_BLOCKER_180D'].sum()) / members_per_year,
    'avg_fills_opioid': (df.groupby('year')['CNT_FILLS_OPIOID_90D'].sum() + df.groupby('year')['CNT_FILLS_OPIOID_180D'].sum()) / members_per_year,
    'mean_health_delta_30d': df.groupby('year')['NOTE_HEALTH_DELTA_30D'].mean(),
    'mean_risk_harm_delta_30d': df.groupby('year')['NOTE_RISK_HARM_DELTA_30D'].mean(),
    'mean_social_stab_delta_30d': df.groupby('year')['NOTE_SOCIAL_STAB_DELTA_30D'].mean(),
    'mean_med_adherence_delta_30d': df.groupby('year')['NOTE_MED_ADHERENCE_DELTA_30D'].mean(),
    'mean_care_engagement_delta_30d': df.groupby('year')['NOTE_CARE_ENGAGEMENT_DELTA_30D'].mean()
})
summary.index.name = 'year'
summary.to_csv('output/claims_dataset_yearly_summary.csv')
print("\nYearly summary saved to output/claims_dataset_yearly_summary.csv")
