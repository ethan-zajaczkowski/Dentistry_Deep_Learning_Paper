import pandas as pd
import subprocess

# Loading Excel files
demographic_data = pd.read_excel('Data/DemographicData_snapshot.xlsx')
pockets_data = pd.read_excel('Data/Pockets_snapshot.xlsx')
missing_ss_data = pd.read_excel('Data/Missing_snapshot.xlsx')
recessions_data = pd.read_excel('Data/Recessions_snapshot.xlsx')

# Define the columns to check
columns_to_check = missing_ss_data.loc[:, 'T18 NOTES':'T28 SURFACES'].columns

# Convert the data to long format for easier filtering
missing_data_long = missing_ss_data.melt(id_vars=['ResearchID', 'CHART DATE'], 
                                         value_vars=columns_to_check, 
                                         var_name='Tooth_Column', 
                                         value_name='Value')

# Split into "Missing" and "Else" entries
missing_entries = missing_data_long[missing_data_long['Value'] == 'Missing']
else_entries = missing_data_long[(missing_data_long['Value'] != 'Missing') & (pd.notna(missing_data_long['Value']))]

# Group and format the "Missing" entries, keeping only the first instance per ResearchID and Tooth Column
missing_grouped = (
    missing_entries.sort_values(['ResearchID', 'CHART DATE'])
    .drop_duplicates(subset=['ResearchID', 'Tooth_Column'], keep='first')
    .groupby('ResearchID')
    .apply(
        lambda x: "<br>".join(
            f"{row['CHART DATE']} - {row['Tooth_Column']} - {row['Value']}"
            for _, row in x.iterrows()
        )
    )
    .reset_index(name='Missing Issues')
)

# Group and format the "Else" entries, keeping only the first instance per ResearchID and Tooth Column
else_grouped = (
    else_entries.sort_values(['ResearchID', 'CHART DATE'])
    .drop_duplicates(subset=['ResearchID', 'Tooth_Column'], keep='first')
    .groupby('ResearchID')
    .apply(
        lambda x: "<br>".join(
            f"{row['CHART DATE']} - {row['Tooth_Column']} - {row['Value']}"
            for _, row in x.iterrows()
        )
    )
    .reset_index(name='Other Issues')
)

# Merge the two grouped results
final_grouped = pd.merge(missing_grouped, else_grouped, on='ResearchID', how='outer')

# Fill any NaN values in missing or other issues with appropriate placeholders
final_grouped['Missing Issues'].fillna("No missing teeth", inplace=True)
final_grouped['Other Issues'].fillna("No other issues", inplace=True)

# Define the output HTML file path
html_file_path = "missing_snapshots_summary.html"

# Open the file for writing
with open(html_file_path, "w") as file:
    # HTML header with main title and styles
    file.write("""
    <html>
    <head>
        <title>Patient History Report</title>
        <style>
            body { 
                font-family: Arial, sans-serif;
                margin: 0; padding: 0; width: 100%;
            }
            .container {
                width: 85%; margin: 0 auto;
            }
            table { 
                width: 100%; border-collapse: collapse;
            }
            th, td { 
                border: 2px solid #333333; padding: 8px; text-align: left;
            }
            th { 
                font-weight: bold; background-color: #f0f0f0;
            }
            td:first-child, th:first-child {
                width: 15%;
            }
            td:nth-child(2), th:nth-child(2), td:nth-child(3), th:nth-child(3) {
                width: 42.5%;
            }
            h1 {
                font-size: 36px; /* Increase font size for main title */
                font-weight: bold; 
                text-align: center; /* Center the main title */
                margin-top: 20px; 
                text-decoration: underline; /* Add underline */
                color: #333333;
            }
            h2 {
                font-size: 24px;
                font-weight: bold;
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
    file.write("<h1>Patient Report History</h1>")
    
    # Teeth Data Summary content
    file.write("<div class='container'>")
    file.write("<h2>Patient Report History</h2>")
    file.write("<p>Summaries of missing teeth and other reported issues for each ResearchID.</p>")
    
    # Write final_grouped DataFrame to HTML
    file.write(final_grouped.to_html(index=False, escape=False))
    
    # Close container and body tags
    file.write("</div></body></html>")

# Attempt to open the file in Safari
try:
    subprocess.run(["open", "-a", "Safari", html_file_path])
    print(f"HTML file created and opened in Safari: {html_file_path}")
except Exception as e:
    print("Could not open Safari. Error:", e)
