import pandas as pd
import subprocess
import numpy as np
import re

### --- Loading Excel files --- ###
demographic_data = pd.read_excel('Data/DemographicData_snapshot.xlsx')
pockets_data = pd.read_excel('Data/Pockets_snapshot.xlsx')
missing_ss_data = pd.read_excel('Data/Missing_snapshot.xlsx')
recessions_data = pd.read_excel('Data/Recessions_snapshot.xlsx')

### --- Define Columns and Patterns --- ###

column_range = recessions_data.loc[:, 'Tooth 18 B':'Tooth 28 P'].columns # These are the columns we would like to check.
pattern = re.compile(r'^\s*(\d{1,2})\s+(\d{1,2})\s+(\d{1,2})\s*$') # Standard format of most of the dataset: "x  y  z "
recessions_data[column_range] = recessions_data[column_range].replace(r'^\s*$', np.nan, regex=True) # Standardize empty or whitespace-only values to NaN

### --- Datatype 1: No Missing Data --- ###
# Rows where all teeth data exists with no missing values 

def row_matches_pattern(row):
    # Function to check if all values in the row pattern above #
    return all(pattern.match(str(row[col])) for col in column_range)
no_missing_data = recessions_data[recessions_data.apply(row_matches_pattern, axis=1)]
no_missing_ids = no_missing_data['ResearchID'].unique()
recessions_data = recessions_data[~recessions_data['ResearchID'].isin(no_missing_ids)] 

### --- Datatype 2: Integer-Type Missing Data --- ###
# Rows with some cells not matching the pattern
def row_does_not_match_pattern(row):
    for col in column_range:
        cell_value = row[col]
        if pd.isna(cell_value) or cell_value == "":
            continue  # Empty cells are treated as matching
        if not pattern.match(str(cell_value)):
            return True
    return False

systematic_missing_data = recessions_data[recessions_data.apply(row_does_not_match_pattern, axis=1)]
systematic_missing_ids = systematic_missing_data['ResearchID'].unique()
recessions_data = recessions_data[~recessions_data['ResearchID'].isin(systematic_missing_ids)]

### --- Datatype 3: Consistent Missing Data --- ###
# ResearchIDs with consistent missing values across multiple visits
def is_fully_consistent_missing_teeth(row):
    research_id = row['ResearchID']
    group = recessions_data[recessions_data['ResearchID'] == research_id]
    if len(group) <= 1:
        return False
    for col in column_range:
        if group[col].isna().all():
            continue
        elif not group[col].isna().all() and not group[col].notna().all():
            return False
    return True

consistent_missing_data = recessions_data[recessions_data.apply(is_fully_consistent_missing_teeth, axis=1)]
consistent_missing_ids = consistent_missing_data['ResearchID'].unique()
recessions_data = recessions_data[~recessions_data['ResearchID'].isin(consistent_missing_ids)]

### --- Datatype 4: Inconsistent Missing Data --- ###
# ResearchIDs with inconsistent missing data across visits
def is_inconsistent_missing_teeth(row):
    research_id = row['ResearchID']
    group = recessions_data[recessions_data['ResearchID'] == research_id]
    if len(group) <= 1:
        return False
    for col in column_range:
        if not group[col].isna().all() and not group[col].notna().all():
            return True
    return False

inconsistent_missing_data = recessions_data[recessions_data.apply(is_inconsistent_missing_teeth, axis=1)]
inconsistent_ids = inconsistent_missing_data['ResearchID'].unique()
recessions_data = recessions_data[~recessions_data['ResearchID'].isin(inconsistent_ids)]

### --- Datatype 5: Missing Data with Single Visit --- ###
single_observation_data = recessions_data.groupby('ResearchID').filter(lambda x: len(x) == 1)
single_observation_ids = single_observation_data['ResearchID'].unique()
recessions_data = recessions_data[~recessions_data['ResearchID'].isin(single_observation_ids)]

# ### --- Datatype 6: Remaining Data --- ###
remaining_data = recessions_data

### --- HTML Report Generation --- ###
def format_integer_missing_teeth_data(group, column_range):
    group = group.sort_values(by='CHART DATE')
    date_missing_teeth = {}
    date_missing_integer_values = {}
    
    for _, row in group.iterrows():
        chart_date = row['CHART DATE']
        if chart_date not in date_missing_teeth:
            date_missing_teeth[chart_date] = []
            date_missing_integer_values[chart_date] = []
        
        for col in column_range:
            cell_value = row[col]
            if pd.isna(cell_value):
                date_missing_teeth[chart_date].append(col)
            elif not pattern.match(str(cell_value)):
                date_missing_integer_values[chart_date].append(col)
    
    formatted_lines = []
    for date in date_missing_teeth.keys():
        missing_teeth_str = f"{date} - Missing: ({', '.join(date_missing_teeth[date])})" if date_missing_teeth[date] else ""
        missing_integer_values_str = f" ---> Missing Integer Values: ({', '.join(date_missing_integer_values[date])})<br>" if date_missing_integer_values[date] else ""
        combined_str = "<br>".join(filter(None, [missing_teeth_str, missing_integer_values_str]))
        formatted_lines.append(combined_str)
    
    return "<br>".join(formatted_lines)

def create_summary_df(data, column_range):
    summary_data = []
    for research_id, group in data.groupby('ResearchID'):
        missing_data_summary = format_integer_missing_teeth_data(group, column_range)
        summary_data.append({'ResearchID': research_id, 'Reported Issues': missing_data_summary})
    return pd.DataFrame(summary_data)

# Generating summaries for each datatype
no_missing_data_summary = create_summary_df(no_missing_data, column_range)
consistent_missing_teeth_summary = create_summary_df(consistent_missing_data, column_range)
single_observation_summary = create_summary_df(single_observation_data, column_range)
systematic_missing_summary = create_summary_df(systematic_missing_data, column_range)
inconsistent_missing_summary = create_summary_df(inconsistent_missing_data, column_range)
remaining_summary = create_summary_df(remaining_data, column_range)

# HTML Report Writing with Custom Style
html_file_path = "recessions_snapshots.html"
with open(html_file_path, "w") as file:
    file.write("""
    <html>
    <head>
        <title>Recessions Data Summary</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 0; padding: 0; width: 100%; }
            .container { width: 85%; margin: 0 auto; }
            table { width: 100%; border-collapse: collapse; }
            th, td { border: 2px solid #333333; padding: 8px; text-align: left; }
            th { font-weight: bold; background-color: #f0f0f0; }
            td:first-child, th:first-child { width: 15%; }
            td:nth-child(2), th:nth-child(2) { width: 85%; }
            h1 {
                font-size: 36px; /* Larger font size for the main title */
                font-weight: bold;
                text-align: center; /* Center the main title */
                margin-top: 20px; /* Add some margin at the top */
                text-decoration: underline; /* Add underline to the main title */
                color: #333333; /* Dark color for the main title */
            }
            h2 { font-size: 24px; font-weight: bold; margin-bottom: 5px; color: #333333; }
            p { font-style: italic; margin-top: 0px; color: #666666; }
        </style>
    </head>
    <body>
    """)

    sections = [
        ("Datatype 1: No Missing Data", "These patients have complete records with no missing data across all teeth.", no_missing_data_summary),
        ("Datatype 2: Consistent Missing Teeth Data", "Patients with consistent missing data across multiple visits for specific teeth.", consistent_missing_teeth_summary),
        ("Datatype 3: Single Observation ResearchIDs", "Patients who only have one visit on record, limiting insights on data consistency.", single_observation_summary),
        ("Datatype 4: Integer-Type Missing Data", "These records contain systematic errors where integer values are missing or improperly formatted.", systematic_missing_summary),
        ("Datatype 5: Inconsistent Missing Data", "Records with inconsistent missing data patterns across visits.", inconsistent_missing_summary),
        ("Datatype 6: Other Data (Remaining)", "Records requiring further investigation due to unique data patterns.", remaining_summary)
    ]

    file.write("<h1>Teeth Recessions Data Summary</h1>")

    for title, description, summary_df in sections:
        file.write(f"<div class='container'><h2>{title}</h2><p>{description}</p>")
        file.write(summary_df.to_html(index=False, escape=False))
        file.write("</div><br>")

    file.write("</body></html>")

# Open the HTML file in Safari
try:
    subprocess.run(["open", "-a", "Safari", html_file_path])
    print(f"HTML file created and opened in Safari: {html_file_path}")
except Exception as e:
    print(f"Could not open Safari. Error: {e}")