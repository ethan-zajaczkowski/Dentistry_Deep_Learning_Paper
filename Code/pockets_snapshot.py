import pandas as pd
import subprocess
import numpy as np
import re
import pickle

### --- Loading Excel files --- ###
demographic_data = pd.read_excel('Data/DemographicData_snapshot.xlsx')
pockets_data = pd.read_excel('Data/Pockets_snapshot.xlsx')
missing_ss_data = pd.read_excel('Data/Missing_snapshot.xlsx')
recessions_data = pd.read_excel('Data/Recessions_snapshot.xlsx')

### --- Pockets Snapshot Summary --- ###

## --- Parameter Setting -- ##
column_range = pockets_data.loc[:, 'Tooth 18 B':'Tooth 28 P'].columns # These are the columns we would like to check.
pattern = re.compile(r'^\s*\d{1,2}(?:\s{1,2}\d{1,2}){2}\s*$') # Standard format of most of the dataset: "x  y  z "
pockets_data[column_range] = pockets_data[column_range].replace(r'^\s*$', np.nan, regex=True) # Standardize empty or whitespace-only values to NaN

## --- Datatype 1: No Missing Data At All --- ##
## All teeth data exists per patient; no missingness. ##

def row_matches_pattern(row):
    # Function to check if all values in the row pattern above #
    return all(pattern.match(str(row[col])) for col in column_range)
no_missing_data = pockets_data[pockets_data.apply(row_matches_pattern, axis=1)]
no_missing_ids = no_missing_data['ResearchID'].unique()
pockets_data = pockets_data[~pockets_data['ResearchID'].isin(no_missing_ids)] 


## --- Datatype 2: Integer-Type Missing Data --- ##
## Patients that have cells that are not complete with three integers ##

def row_does_not_match_pattern(row):
    # Check each cell in the row to see if it is not NaN and does not match the pattern
    for col in column_range:
        cell_value = row[col]
        # If the cell is not NaN and does not match the pattern, it does not match
        if pd.notna(cell_value) and not pattern.match(str(cell_value)):
            return True
    return False
systematic_missing_data = pockets_data[pockets_data.apply(row_does_not_match_pattern, axis=1)]
systematic_missing_ids = systematic_missing_data['ResearchID'].unique()
pockets_data = pockets_data[~pockets_data['ResearchID'].isin(systematic_missing_ids)]

## --- Datatype 3: Consistent Missing Data --- ##
## Patients where the data for missing teeth is consistent among all visits ##

def is_fully_consistent_missing_teeth(row):
    research_id = row['ResearchID']
    group = pockets_data[pockets_data['ResearchID'] == research_id]
    if len(group) <= 1:
        return False
    for col in column_range:
        if group[col].isna().all():
            continue
        elif not group[col].isna().all() and not group[col].notna().all():
            return False  # Inconsistent missing data in this column
    return True
consistent_missing_data = pockets_data[pockets_data.apply(is_fully_consistent_missing_teeth, axis=1)]
consistent_missing_ids = consistent_missing_data['ResearchID'].unique()
pockets_data = pockets_data[~pockets_data['ResearchID'].isin(consistent_missing_ids)]


## --- Datatype 4: Inconsistent Missing Data --- ##
## Patients where the data for missing teeth is not consistent among all visits; some visits report the teeth, others do not. ##

def is_inconsistent_missing_teeth(row):
    research_id = row['ResearchID']
    group = pockets_data[pockets_data['ResearchID'] == research_id]
    # Only consider multi-observation ResearchIDs
    if len(group) <= 1:
        return False
    # Check each column for inconsistent missing data
    for col in column_range:
        # If a column has a mix of NaN and non-NaN values, mark as inconsistent
        if not group[col].isna().all() and not group[col].notna().all():
            return True  # Inconsistent data in this column
    return False
inconsistent_missing_data = pockets_data[pockets_data.apply(is_inconsistent_missing_teeth, axis=1)]
inconsistent_ids = inconsistent_missing_data['ResearchID'].unique()
pockets_data = pockets_data[~pockets_data['ResearchID'].isin(inconsistent_ids)]


## --- Datatype 5: Missing Data But Only One Visit --- ##
## Patients have missing teeth data, but they only visited the dentist once, so cannot conclude if data is truly missing. ##
single_observation_data = pockets_data.groupby('ResearchID').filter(lambda x: len(x) == 1)
single_observation_ids = single_observation_data['ResearchID'].unique()
pockets_data = pockets_data[~pockets_data['ResearchID'].isin(single_observation_ids)]


## --- Datatype 6: Other --- ##
## Require further investigation. ##
remaining_data = pockets_data


### --- HTML Coding Below --- ###
def format_integer_missing_teeth_data(group, column_range):
    # Sort the group by 'CHART DATE' to ensure entries are ordered by date
    group = group.sort_values(by='CHART DATE')
    
    # Dictionary to accumulate missing teeth by date
    date_missing_teeth = {}
    date_missing_integer_values = {}
    
    for _, row in group.iterrows():
        chart_date = row['CHART DATE']
        if chart_date not in date_missing_teeth:
            date_missing_teeth[chart_date] = []
            date_missing_integer_values[chart_date] = []
        
        # Add each missing or incomplete tooth for the current date
        for col in column_range:
            cell_value = row[col]
            if pd.isna(cell_value):
                # Fully missing tooth data
                date_missing_teeth[chart_date].append(col)
            elif not pattern.match(str(cell_value)):
                # Tooth data with missing or incorrect integer format
                date_missing_integer_values[chart_date].append(col)
    
    # Format each date's missing teeth and missing integer values into single lines
    formatted_lines = []
    for date in date_missing_teeth.keys():
        missing_teeth_str = f"{date} - Missing: ({', '.join(date_missing_teeth[date])})" if date_missing_teeth[date] else ""
        missing_integer_values_str = f" ---> Missing Integer Values: ({', '.join(date_missing_integer_values[date])})<br>" if date_missing_integer_values[date] else ""
        
        # Combine both missing teeth and missing integer values for this date
        combined_str = "<br>".join(filter(None, [missing_teeth_str, missing_integer_values_str]))
        formatted_lines.append(combined_str)
    
    return "<br>".join(formatted_lines)


# Function to create a summary DataFrame for each datatype, with HTML line breaks for each entry
def create_summary_df(data, column_range):
    summary_data = []
    for research_id, group in data.groupby('ResearchID'):
        # Generate the missing data summary for each ResearchID
        missing_data_summary = format_integer_missing_teeth_data(group, column_range)
        summary_data.append({'ResearchID': research_id, 'Reported Issues': missing_data_summary})
    return pd.DataFrame(summary_data)

no_missing_data_summary = create_summary_df(no_missing_data, column_range)
consistent_missing_teeth_summary = create_summary_df(consistent_missing_data, column_range)
single_observation_summary = create_summary_df(single_observation_data, column_range)
systematic_missing_summary = create_summary_df(systematic_missing_data, column_range)
inconsistent_missing_summary = create_summary_df(inconsistent_missing_data, column_range)
remaining_summary = create_summary_df(remaining_data, column_range)

# Write the summaries to a single HTML file with line breaks for formatted display
html_file_path = "pockets_snapshot.html"
with open(html_file_path, "w") as file:
    # Writing the HTML header with enhanced styles
    file.write("""
    <html>
    <head>
        <title>Missing Teeth Data Summary</title>
        <style>
            body { 
                font-family: Arial, sans-serif;
                margin: 0; /* Remove default body margin */
                padding: 0; /* Remove default body padding */
                width: 100%; /* Make content span the full width */
            }
            .container {
                width: 85%; /* Adjust width to control alignment */
                margin: 0 auto; /* Center-align content */
            }
            table { 
                width: 100%; /* Set table to full width */
                border-collapse: collapse; 
            }
            th, td { 
                border: 2px solid #333333; /* Darker border color */
                padding: 8px; 
                text-align: left; 
            }
            th { 
                font-weight: bold; 
                background-color: #f0f0f0; 
            } /* Light background for headers */
            td:first-child, th:first-child {
                width: 15%; /* Adjust percentage for ResearchID column */
            }
            td:nth-child(2), th:nth-child(2) {
                width: 85%;
            }
            h1 {
                font-size: 36px; /* Larger font size for the main title */
                font-weight: bold;
                text-align: center; /* Center the main title */
                margin-top: 20px; /* Add some margin at the top */
                text-decoration: underline; /* Add underline to the main title */
                color: #333333; /* Dark color for the main title */
            }
            h2 {
                font-size: 24px; /* Font size for section titles */
                font-weight: bold;
                margin-bottom: 5px;
                color: #333333;
            }
            p {
                font-style: italic;
                margin-top: 0px;
                color: #666666;
            }
        </style>
    </head>
    <body>
    """)

    # Add the main title
    file.write("<h1>Missing Teeth Data Summary</h1>")  # Main title with enhanced styling
    
    # Add a break and start the main content container
    file.write("<div class='container'>")

    # No Missing Data Summary (Full information)
    file.write("<h2>Datatype 1: No Missing Data</h2>")
    file.write("<p>These patients have complete records with no missing data across all teeth.</p>")
    file.write(no_missing_data_summary.to_html(index=False, escape=False))
    file.write("</div><br>")

    # Consistently Missing Teeth Summary
    file.write("<div class='container'>")
    file.write("<h2>Datatype 2: Consistently Missing Teeth Data</h2>")
    file.write("<p>Patients with consistent missing data across multiple visits for specific teeth.</p>")
    file.write(consistent_missing_teeth_summary.to_html(index=False, escape=False))
    file.write("</div><br>")

    # Single Observation ResearchIDs
    file.write("<div class='container'>")
    file.write("<h2>Datatype 3: Single Observation ResearchIDs</h2>")
    file.write("<p>Patients who only have one visit on record, limiting insights on data consistency.</p>")
    file.write(single_observation_summary.to_html(index=False, escape=False))
    file.write("</div><br>")

    # Integer-Type Missing Data (systematic)
    file.write("<div class='container'>")
    file.write("<h2>Datatype 4: Integer-Type Missing Data</h2>")
    file.write("<p>These records contain systematic errors where integer values are missing or improperly formatted.</p>")
    file.write(systematic_missing_summary.to_html(index=False, escape=False))
    file.write("</div><br>")

    # Inconsistent Missing Data
    file.write("<div class='container'>")
    file.write("<h2>Datatype 5: Inconsistent Missing Data</h2>")
    file.write("<p>Records with inconsistent missing data patterns across visits.</p>")
    file.write(inconsistent_missing_summary.to_html(index=False, escape=False))
    file.write("</div><br>")

    # Other Data
    file.write("<div class='container'>")
    file.write("<h2>Datatype 6: Other Data (Remaining)</h2>")
    file.write("<p>Records requiring further investigation due to unique data patterns.</p>")
    file.write(remaining_summary.to_html(index=False, escape=False))
    file.write("</div>")

    # Closing the HTML tags
    file.write("</body></html>")

try:
    subprocess.run(["open", "-a", "Safari", html_file_path])
    print(f"HTML file created and opened in Safari: {html_file_path}")
except Exception as e:
    print(f"Could not open Safari. Error: {e}")

# Creating a pickel file for cross-validation use.

# Add a Data_Type column to each subset
no_missing_data["Data_Type"] = "No Missing Data"
systematic_missing_data["Data_Type"] = "Systematic Missing Data"
consistent_missing_data["Data_Type"] = "Consistent Missing Data"
inconsistent_missing_data["Data_Type"] = "Inconsistent Missing Data"
single_observation_data["Data_Type"] = "Single Observation Data"
remaining_data["Data_Type"] = "Other (Remaining Data)"

# Concatenate all subsets into one DataFrame
pickle_df = pd.concat([no_missing_data, 
                       systematic_missing_data, 
                       consistent_missing_data, 
                       inconsistent_missing_data, 
                       single_observation_data, 
                       remaining_data], 
                      ignore_index=True)

# Save the concatenated DataFrame as a pickle file
with open("Data_PKL/pockets_snapshots.pkl", "wb") as f:
    pickle.dump(pickle_df, f)