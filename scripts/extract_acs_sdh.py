# Debug: Print selected ACS columns and friendly names
print("Selected ACS columns for SDoH factors:")
for code, name in factor_codes.items():
    print(f"{code}: {name}")
import pandas as pd

import pandas as pd
import os
import sys
import traceback

# Paths

acs_data_path = '/Users/ananth/Personal AI Projects/Risk Grouper - Development/Public Files/ACSST5Y2023.S1701_2025-08-23T191746/ACSST5Y2023.S1701-Data.csv'
metadata_path = '/Users/ananth/Personal AI Projects/Risk Grouper - Development/Public Files/ACSST5Y2023.S1701_2025-08-23T191746/ACSST5Y2023.S1701-Column-Metadata.csv'
output_path = 'output/sdoh_factors.csv'

# Read ACS data and metadata
acs_df = pd.read_csv(acs_data_path, low_memory=False)
metadata_df = pd.read_csv(metadata_path)

# Define key SDoH indicators most likely to impact healthcare
indicator_keywords = [
    'percent below poverty',
    'snap',
    'food stamp',
    'less than high school',
    'unemploy',
    'insurance',
    'health',
    'income',
]





# Step 1: Select SDoH columns and create user-friendly names
factor_info = []
for keyword in indicator_keywords:
    matches = metadata_df[metadata_df.apply(lambda row: row.astype(str).str.contains(keyword, case=False, na=False).any(), axis=1)]
    for _, row in matches.iterrows():
        col_code = row['Column Code'] if 'Column Code' in row else row.get('Column Code', None)
        col_label = row['Label'] if 'Label' in row else row.get('Label', None)
        if col_code and col_code in acs_df.columns:
            friendly_name = col_label if col_label else keyword.title()
            factor_info.append({'code': col_code, 'name': friendly_name, 'keyword': keyword})

# Always include geography column
geo_col = 'NAME' if 'NAME' in acs_df.columns else None

# Step 2: Build dataframe and calculate outlier flags
factor_codes = [f['code'] for f in factor_info]
factor_names = [f['name'] for f in factor_info]
factor_keywords = [f['keyword'] for f in factor_info]

factor_data = acs_df[factor_codes].apply(pd.to_numeric, errors='coerce')
means = factor_data.mean()
stds = factor_data.std()

# Define which direction is 'bad' for each factor
bad_direction = {}
for f in factor_info:
    if any(k in f['keyword'] for k in ['poverty', 'snap', 'food stamp', 'unemploy']):
        bad_direction[f['code']] = 'high'
    elif 'education' in f['keyword']:
        bad_direction[f['code']] = 'low'
    elif any(k in f['keyword'] for k in ['insurance', 'health', 'income']):
        bad_direction[f['code']] = 'low'
    else:
        bad_direction[f['code']] = 'high'

output_rows = []
geo_values = acs_df[geo_col] if geo_col else acs_df.index.astype(str)
for idx, geo in enumerate(geo_values):
    row = {'Geography': geo}
    for code, name, keyword in zip(factor_codes, factor_names, factor_keywords):
        value = factor_data.iloc[idx][code]
        mean = means[code]
        std = stds[code]
        flag = 0
        if pd.notnull(value):
            if bad_direction[code] == 'high':
                if value > mean + std:
                    flag = 1
            elif bad_direction[code] == 'low':
                if value < mean - std:
                    flag = 1
        row[name + ' Outlier'] = flag
    output_rows.append(row)

output_df = pd.DataFrame(output_rows)
os.makedirs(os.path.dirname(output_path), exist_ok=True)
try:
    output_df.to_csv(output_path, index=False)
except Exception as e:
    with open('output/sdoh_error.log', 'w') as log:
        log.write(traceback.format_exc())
    print(f"Error occurred. See output/sdoh_error.log for details.", file=sys.stderr)



# ...existing code...
os.makedirs(os.path.dirname(output_path), exist_ok=True)
try:
    output_df.to_csv(output_path, index=False)
except Exception as e:
    with open('output/sdoh_error.log', 'w') as log:
        log.write(traceback.format_exc())
    print(f"Error occurred. See output/sdoh_error.log for details.", file=sys.stderr)
