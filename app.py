import pandas as pd
import os
import glob
import sys

# Check for and delete existing combined_output.csv
if os.path.exists('combined_output.csv'):
    print("Found existing combined_output.csv - deleting it...")
    os.remove('combined_output.csv')

# Check for and delete existing combined_output.csv
if os.path.exists('office_totals.csv'):
    print("Found existing office_totals.csv - deleting it...")
    os.remove('office_totals.csv')

# Find CSV files in current directory
csv_files = glob.glob('*.csv')

if not csv_files:
    raise FileNotFoundError("No CSV files found in the current directory")

# Create an empty list to store all processed dataframes
all_dfs = []

# Process each CSV file
for input_file in csv_files:
    # Skip the office_totals.csv file
    if input_file == 'office_totals.csv':
        continue
        
    print(f"Processing {input_file}...")
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Keep only the specified columns
    columns_to_keep = ['Display name', 'Office', 'Licenses']
    df = df[columns_to_keep]
    
    # Check for empty offices and warn
    empty_offices = df[df['Office'].isna() | (df['Office'] == '')]
    if not empty_offices.empty:
        print("\nERROR: The following users have no office assigned:")
        for _, row in empty_offices.iterrows():
            print(f"- {row['Display name']}")
        print("\nPlease assign offices to these users and try again.")
        sys.exit(1)  # Exit with error code 1
    
    # Add to our list of dataframes
    all_dfs.append(df)

# Combine all dataframes into one
final_df = pd.concat(all_dfs, ignore_index=True)

# Remove any duplicate rows
final_df = final_df.drop_duplicates()

# Clean the Licenses column - keep only the specified values within each cell
def clean_licenses(licenses_str):
    if pd.isna(licenses_str):
        return ''
    
    # Define valid licenses
    valid_licenses = [
        'Power BI Pro',
        'Visio Plan 2',
        'Power BI Premium Per User'
    ]
    
    # Split the licenses string using '+' delimiter
    licenses_list = licenses_str.split('+')
    
    # Keep only valid licenses and join them back together
    valid = []
    for lic in licenses_list:
        lic = lic.strip()
        # Exact matching instead of substring matching
        if lic.lower() in (valid_lic.lower() for valid_lic in valid_licenses):
            valid.append(lic)
    
    return '+'.join(valid) if valid else ''

final_df['Licenses'] = final_df['Licenses'].apply(clean_licenses)

# Save the combined DataFrame to a new CSV file
final_df.to_csv('combined_output.csv', index=False)

# Create new DataFrame for office totals
office_totals_df = pd.DataFrame(columns=['Office', 'Licenses', 'Totals'])
office_totals_df.to_csv('office_totals.csv', index=False)

print(f"Processed {len(csv_files)} files. Results saved to 'combined_output.csv'")
print(f"Office totals template created in 'office_totals.csv'")

# After creating office_totals.csv, process combined_output for totals
combined_df = pd.read_csv('combined_output.csv')
office_totals_rows = []

# Process each row in combined_output.csv
for _, row in combined_df.iterrows():
    office = row['Office']
    licenses = row['Licenses']
    
    # Skip if no licenses
    if pd.isna(licenses) or licenses == '':
        continue
        
    # Split licenses and create a row for each
    for license in licenses.split('+'):
        if license.strip():  # Only process non-empty licenses
            office_totals_rows.append({
                'Office': office,
                'Licenses': license.strip(),
                'Totals': 1
            })

# Create DataFrame from collected rows and save
if office_totals_rows:
    office_totals_df = pd.DataFrame(office_totals_rows)
    # Group by Office and License to get totals
    office_totals_df = office_totals_df.groupby(['Office', 'Licenses'])['Totals'].sum().reset_index()
    office_totals_df.to_csv('office_totals.csv', index=False)

print("Office totals have been calculated and saved to office_totals.csv")

def process_data(df):
    # Group by display name and aggregate other columns
    grouped_df = df.groupby('Display Name', as_index=False).agg({
        'Office': lambda x: '+'.join(sorted(set(x.dropna()))),  # Unique offices
        'Licenses': lambda x: '+'.join(sorted(set(filter(None, x.dropna()))))  # Unique licenses
    })
    
    # Clean the licenses for each row
    grouped_df['Licenses'] = grouped_df['Licenses'].apply(clean_licenses)
    
    return grouped_df




