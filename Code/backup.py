import pickle
import pandas as pd
import re
from datetime import datetime
import subprocess

# Load Data
with open("Data_PKL/missing_snapshots.pkl", "rb") as f:
    missing_snapshots_df = pickle.load(f)

with open("Data_PKL/pockets_snapshots.pkl", "rb") as f:
    pockets_snapshots_df = pickle.load(f)

with open("Data_PKL/recessions_snapshots.pkl", "rb") as f:
    recessions_snapshots_df = pickle.load(f)    

### --- All Functions --- ###

# Replace Tooth Codes
def replace_tooth_codes(text):
    return re.sub(r'\bT(\d{1,2})\b', r'Tooth \1', text)

# Extract Missing Teeth
def extract_missing_teeth_with_dates(missing_issues):
    if isinstance(missing_issues, str):
        matches = re.findall(r'(\d{4}-\d{2}-\d{2}) - Tooth (\d{1,2})', missing_issues)
        return [(datetime.strptime(date, '%Y-%m-%d'), tooth) for date, tooth in matches]
    return []

# Record Missing Teeth
def record_cells(row):
    missing_teeth_dates = row['Missing Teeth with Dates']
    chart_date = datetime.strptime(row['CHART DATE'], '%Y-%m-%d')
    
    missing_pockets_and_record = set()
    missing_pockets_not_record = set()
    missing_pockets_other_issue = set()
    integer_error = set()

    # Define the pattern for the standard format "x y z"
    standard_pattern = re.compile(r'^\s*\d{1,2}(?:\s{1,2}\d{1,2}){2}\s*$')

    for col in row.index:
        if col.startswith("Tooth") and (" P" in col or " B" in col):
            tooth_num = re.search(r'Tooth (\d{1,2})', col).group(1)
            found = False

            # Check for cells not matching the standard pattern
            if pd.notna(row[col]) and not standard_pattern.match(str(row[col])):
                integer_error.add(f"{col}")

            # Check for missing teeth confirmed in records
            for missing_date, tooth in missing_teeth_dates:
                if tooth == tooth_num and missing_date <= chart_date:
                    missing_pockets_and_record.add(f"{col}")
                    found = True
                    break

            if found:
                continue  # Skip other checks if already recorded
                
            # Check for NaN values not recorded elsewhere
            elif pd.isna(row[col]):
                if not any(tooth == tooth_num for _, tooth in missing_teeth_dates):
                    missing_pockets_not_record.add(f"{col}")
                else:
                    missing_pockets_other_issue.add(f"{col}")

    # Update columns with recorded information, sorted to maintain order
    combined_df.at[row.name, 'Missing Teeth In Pockets Data, And Is Recorded In Patient Report (Likely Missing)'] = ', '.join(sorted(missing_pockets_and_record))
    combined_df.at[row.name, 'Missing Teeth In Pockets Data, But Not In Patient Report'] = ', '.join(sorted(missing_pockets_not_record))
    combined_df.at[row.name, 'Missing Teeth In Pockets Data, Other Issues'] = ', '.join(sorted(missing_pockets_other_issue))
    combined_df.at[row.name, 'Teeth Integer Data Is Not Complete'] = ', '.join(sorted(integer_error))

# Aggregate issues for each ResearchID group
def aggregate_issues(group):
    def aggregate_column(column_name):
        unique_entries = group[['CHART DATE', column_name]].dropna()
        tooth_dict = {}
        
        for _, row in unique_entries.iterrows():
            date_str = row['CHART DATE']
            entries = row[column_name].split(', ')
            for item in entries:
                match = re.match(r'Tooth (\d{1,2}) (P|B)', item.strip())
                if match:
                    tooth, side = match.groups()
                    if tooth not in tooth_dict:
                        tooth_dict[tooth] = {}
                    # Only add the first occurrence of the date for each side
                    if side not in tooth_dict[tooth]:
                        tooth_dict[tooth][side] = date_str  # Record the first date only

        # Format each tooth with its side and the first date it appears
        formatted_teeth = []
        for tooth, sides in sorted(tooth_dict.items()):
            if 'P' in sides and 'B' in sides:
                date_p = sides['P']
                date_b = sides['B']
                if date_p == date_b:
                    # Same date for both sides
                    formatted_teeth.append(f"Tooth {tooth} - B & P - {date_p}")
                else:
                    formatted_teeth.append(f"Tooth {tooth} - (B - {date_b}) / (P - {date_p})")
            elif 'P' in sides:
                formatted_teeth.append(f"Tooth {tooth} - Only P - {sides['P']}")
            elif 'B' in sides:
                formatted_teeth.append(f"Tooth {tooth} - Only B - {sides['B']}")


        # Join the formatted teeth list with commas and line breaks for HTML
        return '<br>'.join(formatted_teeth).strip(', ')

    return pd.Series({
        'Missing Teeth In Pockets Data, And Is Recorded In Patient Report (Likely Missing)':
            aggregate_column('Missing Teeth In Pockets Data, And Is Recorded In Patient Report (Likely Missing)'),
        'Missing Teeth In Pockets Data, But Not In Patient Report':
            aggregate_column('Missing Teeth In Pockets Data, But Not In Patient Report'),
        'Missing Teeth In Pockets Data, Other Issues':
            aggregate_column('Missing Teeth In Pockets Data, Other Issues'),
        'Teeth Integer Data Is Not Complete':
            aggregate_column('Teeth Integer Data Is Not Complete')
    })

# Highlight Cells Function
def highlight_cells(full_df, index):
    row = full_df.loc[index]
    missing_teeth_dates = row['Missing Teeth with Dates']
    chart_date_str = row['CHART DATE']
    if chart_date_str is None:
        return pd.Series('', index=row.index)
    
    chart_date = datetime.strptime(chart_date_str, '%Y-%m-%d')
    style = pd.Series('', index=row.index)
    
    # Define the standard pattern for validation
    standard_pattern = re.compile(r'^\s*\d{1,2}(?:\s{1,2}\d{1,2}){2}\s*$')

    # Iterate through each cell in the row
    for col in row.index:
        if col.startswith("Tooth") and (" P" in col or " B" in col):
            tooth_match = re.search(r'Tooth (\d{1,2})', col)
            if not tooth_match:
                continue
            
            tooth_num = tooth_match.group(1)
            cell_value = row[col]

            # Case 1: Yellow Highlighting for incorrect integer format
            if pd.notna(cell_value) and not standard_pattern.match(str(cell_value)):
                style[col] = 'background-color: #FFFF00'
                continue  # Skip to next cell since yellow takes precedence

            # Case 2: Green Highlighting for confirmed missing tooth
            is_missing_tooth = any(
                tooth == tooth_num and missing_date <= chart_date
                for missing_date, tooth in missing_teeth_dates
            )
            if is_missing_tooth:
                style[col] = 'background-color: green'
                continue  # Skip to next cell if highlighted green

            # Case 3: Red/Orange Highlighting for NaN values
            if pd.isna(cell_value):
                if is_missing_tooth:
                    style[col] = 'background-color: orange'  # Confirmed missing but NaN
                else:
                    style[col] = 'background-color: lightcoral'  # Missing without confirmation

    return style


### --- Process --- ###

# Merge DataFrames
combined_df = pockets_snapshots_df.merge(
    missing_snapshots_df[['ResearchID', 'Missing Issues']],
    on='ResearchID',
    how='left'
)

combined_df['Missing Issues'] = combined_df['Missing Issues'].apply(
    lambda x: replace_tooth_codes(x) if isinstance(x, str) else x
)

combined_df['CHART DATE'] = pd.to_datetime(combined_df['CHART DATE']).dt.strftime('%Y-%m-%d')
combined_df = combined_df.sort_values(by=['ResearchID', 'CHART DATE']).reset_index(drop=True)
combined_df['Missing Teeth with Dates'] = combined_df['Missing Issues'].apply(extract_missing_teeth_with_dates)

# Initialize Columns
combined_df['Missing Teeth In Pockets Data, And Is Recorded In Patient Report (Likely Missing)'] = ''
combined_df['Missing Teeth In Pockets Data, But Not In Patient Report'] = ''
combined_df['Missing Teeth In Pockets Data, Other Issues'] = ''
combined_df['Teeth Integer Data Is Not Complete'] = ''


combined_df.apply(record_cells, axis=1)

summary_df = combined_df.groupby('ResearchID').apply(aggregate_issues).reset_index()

## Summary Statistics

# Filter combined_df for rows where specific columns are not empty
aggregated_df = combined_df.groupby('ResearchID').apply(aggregate_issues).reset_index()

# Filter the aggregated DataFrame for non-empty columns for the summary report
filtered_aggregated_df = aggregated_df[
    aggregated_df[['Missing Teeth In Pockets Data, But Not In Patient Report',
                   'Missing Teeth In Pockets Data, Other Issues',
                   'Teeth Integer Data Is Not Complete']].apply(lambda x: x.str.strip() != '').any(axis=1)
]

filtered_aggregated_df.to_excel('Data/Final Summary, Pockets Data.xlsx', index=False)

def add_line_breaks(text):
    return '<br>'.join(text.split(', ')) if isinstance(text, str) else text

# Apply the function to each relevant column
filtered_aggregated_df['Missing Teeth In Pockets Data, But Not In Patient Report'] = filtered_aggregated_df['Missing Teeth In Pockets Data, But Not In Patient Report'].apply(add_line_breaks)
filtered_aggregated_df['Missing Teeth In Pockets Data, Other Issues'] = filtered_aggregated_df['Missing Teeth In Pockets Data, Other Issues'].apply(add_line_breaks)
filtered_aggregated_df['Teeth Integer Data Is Not Complete'] = filtered_aggregated_df['Teeth Integer Data Is Not Complete'].apply(add_line_breaks)

### --- HTML Pages --- ###

def generate_research_groups(group_df):
    # Function to generate HTML content for each ResearchID within a Data_Type
    content = ''
    for research_id, research_group in group_df.groupby("ResearchID"):
        summary = summary_df[summary_df['ResearchID'] == research_id].iloc[0]

        # Prepare the table for the research group
        styled_table = (
            research_group.iloc[:, 0:43]
            .style.apply(lambda row: highlight_cells(combined_df, row.name), axis=1)
            .hide(axis="index")
            .hide(columns_to_hide, axis="columns")
            .to_html(index=False, escape=False)
        )

        research_container = f"""
        <div class="research-container" data-researchid="{research_id}">
            <h3>ResearchID {research_id}</h3>
            <p><b>Missing Issues:</b><br>{summary['Missing Teeth In Pockets Data, And Is Recorded In Patient Report (Likely Missing)']}</p>
            <div class='scrollable-table'>{styled_table}</div>
            {generate_static_section(summary)}
        </div>
        """
        content += research_container
    return content

def generate_static_section(summary):
    # Function to generate the static summary table
    static_section = f"""
    <div class="static-section">
        <table>
            <tr>
                <td style="width: 25%; background-color: #d3d3d3;"><b>Missing Teeth In Pockets Data, And Is Recorded In Patient Report (Likely Missing):</b></td>
                <td>{summary['Missing Teeth In Pockets Data, And Is Recorded In Patient Report (Likely Missing)']}</td>
            </tr>
            <tr>
                <td style="width: 25%; background-color: #d3d3d3;"><b>Missing Teeth In Pockets Data, But Not In Patient Report:</b></td>
                <td>{summary['Missing Teeth In Pockets Data, But Not In Patient Report']}</td>
            </tr>
            <tr>
                <td style="width: 25%; background-color: #d3d3d3;"><b>Missing Teeth In Pockets Data, Other Issues:</b></td>
                <td>{summary['Missing Teeth In Pockets Data, Other Issues']}</td>
            </tr>
            <tr>
                <td style="width: 25%; background-color: #d3d3d3;"><b>Teeth Integer Data Is Not Complete:</b></td>
                <td>{summary['Teeth Integer Data Is Not Complete']}</td>
            </tr>
        </table>
    </div>
    """
    return static_section

# List of columns to hide in the tables
columns_to_hide = [
    'Missing Issues', 'Data_Type',
    'Missing Teeth In Pockets Data, And Is Recorded In Patient Report (Likely Missing)',
    'Missing Teeth with Dates', 
    'Missing Teeth In Pockets Data, But Not In Patient Report', 
    'Missing Teeth In Pockets Data, Other Issues', 
    'Teeth Integer Data Is Not Complete'
]

# Generate Summary Page
def generate_summary_page():
    html_file_path = "Data Reports HTML/Cross Validation/summary.html"

    html_template = """
    <html>
    <head>
        <title>Summary Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            .container {{ width: 85%; margin: 0 auto; }}
            .scrollable-table {{ overflow-x: auto; width: 100%; border: 1px solid #333333; margin-bottom: 15px; }}
            table {{ width: 100%; border-collapse: collapse; border: 2px solid #333333; table-layout: fixed; }}
            th, td {{ border: 1px solid #333333; padding: 8px; text-align: left; }}
            th {{ font-weight: bold; background-color: #d3d3d3; }}
            h1 {{ font-size: 36px; font-weight: bold; text-align: center; margin-top: 20px; }}
            h2 {{ font-size: 24px; font-weight: bold; }}
            .navigation-button {{ padding: 10px; width: 120px; background-color: #d3d3d3; color: black; border: none; cursor: pointer; text-align: center; }}
            .header-bar {{ display: flex; align-items: center; justify-content: flex-end; margin-bottom: 0px; margin-right: 105px; margin-left: 105px; }}
            .button-group {{ display: flex; gap: 10px; }}
            th:first-child, td:first-child {{ width: 15%; }}
            th:not(:first-child), td:not(:first-child) {{ width: 28%; }}
        </style>
    </head>
    <body>
        <h1>Summary Report</h1>
        <div class="header-bar">
            <div class="button-group">
                <button class="navigation-button" onclick="window.location.href='analysis.html'">Analysis Page</button>
                <button class="navigation-button" onclick="window.location.href='search.html'">Search Page</button>
            </div>
        </div>
        <div class="container">
            <h2>Summary of Missing Data Issues</h2>
            <p>This table shows records where specific missing data fields are not empty.</p>
            <div class="scrollable-table">
                <table>
                    <thead>
                        <tr>
                            <th>ResearchID</th>
                            <th>Missing Teeth In Pockets Data, But Not In Patient Report</th>
                            <th>Missing Teeth In Pockets Data, Other Issues</th>
                            <th>Teeth Integer Data Is Not Complete</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """

    # Generate table rows
    table_rows = ""
    for _, row in filtered_aggregated_df.iterrows():
        table_rows += f"""
                        <tr>
                            <td>{row['ResearchID']}</td>
                            <td>{row['Missing Teeth In Pockets Data, But Not In Patient Report']}</td>
                            <td>{row['Missing Teeth In Pockets Data, Other Issues']}</td>
                            <td>{row['Teeth Integer Data Is Not Complete']}</td>
                        </tr>
        """

    # Complete the HTML content
    html_content = html_template.format(table_rows=table_rows)

    # Write the HTML content to the file
    with open(html_file_path, "w") as file:
        file.write(html_content)
    
    return html_file_path

def generate_analysis_page():
    data_types = combined_df['Data_Type'].unique()
    html_file_path = "Data Reports HTML/Cross Validation/analysis.html"

    # HTML Templates
    html_template = """
    <html>
    <head>
        <title>Cross Validation Report by Data Type</title>
        <style>
            {styles}
        </style>
        <script>
            {script}
        </script>
    </head>
    <body>
        <h1>Cross Validation Report by Data Type</h1>
        {header_bar}
        {data_type_containers}
    </body>
    </html>
    """

    styles = """
    body { font-family: Arial, sans-serif; }
    .container { width: 85%; margin: 0 auto; display: none; }
    .scrollable-table { overflow-x: auto; width: 100%; border: 1px solid #333333; margin-bottom: 15px; }
    table { width: 100%; border-collapse: collapse; border: 2px solid #333333; }
    th, td { border: 1px solid #333333; padding: 8px; text-align: left; vertical-align: top; }
    th { font-weight: bold; background-color: #d3d3d3; }
    h1 { font-size: 36px; font-weight: bold; text-align: center; margin-top: 20px; }
    h2 { font-size: 24px; font-weight: bold; }
    .research-container { border: 2px solid #333; padding: 15px; margin-bottom: 20px; }
    .navigation-button { padding: 10px; width: 120px; background-color: #d3d3d3; color: black; border: none; cursor: pointer; text-align: center; }
    .header-bar { display: flex; align-items: center; justify-content: space-between; margin: 0 105px; }
    .select-group { display: flex; align-items: center; }
    .button-group { display: flex; gap: 10px; }
    """

    script = """
    function showDataType(dataType) {
        var containers = document.getElementsByClassName('container');
        for (var i = 0; i < containers.length; i++) {
            containers[i].style.display = 'none';
        }
        var selectedContainer = document.getElementById('container-' + dataType);
        if (selectedContainer) {
            selectedContainer.style.display = 'block';
        }
    }
    """

    # Header Bar Template
    header_bar = f"""
    <div class="header-bar">
        <div class="select-group">
            <label for="dataTypeSelect"><b>Select Data Type:</b></label>
            <select id="dataTypeSelect" onchange="showDataType(this.value)">
                <option value="">Select Data Type</option>
                {''.join(f'<option value="{dt}">{dt}</option>' for dt in data_types)}
            </select>
        </div>
        <div class="button-group">
            <button class="navigation-button" onclick="window.location.href='summary.html'">Summary Page</button>
            <button class="navigation-button" onclick="window.location.href='search.html'">Search Page</button>
        </div>
    </div>
    """

    # Generate Data Type Containers
    data_type_containers = ''
    for data_type, group_df in combined_df.groupby("Data_Type"):
        container_content = f"""
        <div class="container" id="container-{data_type}">
            <h2>Data Type: {data_type}</h2>
            {generate_research_groups(group_df)}
        </div>
        """
        data_type_containers += container_content

    # Generate the full HTML content
    html_content = html_template.format(
        styles=styles,
        script=script,
        header_bar=header_bar,
        data_type_containers=data_type_containers
    )

    # Write to the HTML file
    with open(html_file_path, "w") as file:
        file.write(html_content)

    return html_file_path

def generate_search_page():
    html_file_path = "Data Reports HTML/Cross Validation/search.html"

    # HTML Templates
    html_template = """
    <html>
    <head>
        <title>Search Report by ResearchID</title>
        <style>
            {styles}
        </style>
        <script>
            {script}
        </script>
    </head>
    <body>
        <h1>Search Report by ResearchID</h1>
        {header_bar}
        <div id="noResultMessage">No results found for the entered ResearchID.</div>
        {data_type_containers}
    </body>
    </html>
    """

    styles = """
    body { font-family: Arial, sans-serif; }
    .container { width: 85%; margin: 0 auto; display: none; }
    .scrollable-table { overflow-x: auto; width: 100%; border: 1px solid #333333; margin-bottom: 15px; }
    table { width: 100%; border-collapse: collapse; border: 2px solid #333333; }
    th, td { border: 1px solid #333333; padding: 8px; text-align: left; vertical-align: top; }
    th { font-weight: bold; background-color: #d3d3d3; }
    h1 { font-size: 36px; font-weight: bold; text-align: center; margin-top: 20px; }
    h2 { font-size: 24px; font-weight: bold; margin-top: 5px; }
    .research-container { border: 2px solid #333; padding: 15px; margin-bottom: 10px; display: none; }
    .navigation-button { padding: 10px; width: 120px; background-color: #d3d3d3; color: black; border: none; cursor: pointer; text-align: center; }
    .header-bar { display: flex; align-items: center; justify-content: space-between; margin: 0 105px 20px 105px; }
    .select-group { display: flex; align-items: center; gap: 10px; }
    .button-group { display: flex; gap: 10px; }
    .search-button {
        padding: 6px 10px;
        font-size: 11px;
        color: white;
        background-color: #007AFF;
        border: none;
        border-radius: 8px;
        cursor: pointer;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        height: 19px;
        display: flex;
        align-items: center;
    }
    .search-button:hover {
        background-color: #005BBB;
    }
    input[type="text"] {
        height: 19px;
    }
    #noResultMessage {
        display: none;
        text-align: center;
        color: red;
        font-weight: bold;
        margin-top: 15px;
    }
    """

    script = """
    function searchByResearchID() {
        var searchInput = document.getElementById('researchIDInput').value.trim();
        var allContainers = document.getElementsByClassName('container');
        var researchContainers = document.getElementsByClassName('research-container');

        // Hide all containers initially
        for (var i = 0; i < allContainers.length; i++) {
            allContainers[i].style.display = 'none';
        }

        var found = false;

        // Show only the research container that matches the search
        for (var j = 0; j < researchContainers.length; j++) {
            var researchID = researchContainers[j].getAttribute('data-researchid');
            if (researchID === searchInput) {
                researchContainers[j].parentElement.style.display = 'block'; // Show parent container
                researchContainers[j].style.display = 'block';
                found = true;
            } else {
                researchContainers[j].style.display = 'none';
            }
        }

        // Display or hide the 'No Results' message
        var noResultMessage = document.getElementById('noResultMessage');
        noResultMessage.style.display = found ? 'none' : 'block';
    }
    """

    # Header Bar Template
    header_bar = """
    <div class="header-bar">
        <div class="select-group">
            <label for="researchIDInput"><b>Enter ResearchID:</b></label>
            <input type="text" id="researchIDInput" placeholder="Enter ResearchID">
            <button class="search-button" onclick="searchByResearchID()">Search</button>
        </div>
        <div class="button-group">
            <button class="navigation-button" onclick="window.location.href='summary.html'">Summary Report</button>
            <button class="navigation-button" onclick="window.location.href='analysis.html'">Analysis Report</button>
        </div>
    </div>
    """

    # Generate Data Type Containers
    data_type_containers = ''
    for data_type, group_df in combined_df.groupby("Data_Type"):
        container_content = f"""
        <div class="container" id="container-{data_type}">
            <h2>Data Type: {data_type}</h2>
            {generate_research_groups(group_df)}
        </div>
        """
        data_type_containers += container_content

    # Generate the full HTML content
    html_content = html_template.format(
        styles=styles,
        script=script,
        header_bar=header_bar,
        data_type_containers=data_type_containers
    )

    # Write to the HTML file
    with open(html_file_path, "w") as file:
        file.write(html_content)

    return html_file_path

# Generate both pages
analysis_html = generate_analysis_page()
summary_html = generate_summary_page()
search_html = generate_search_page()


# Open the Analysis page in Safari
try:
    subprocess.run(["open", "-a", "Safari", summary_html])
    print(f"HTML files created. Summary page opened in Safari: {summary_html}")
except Exception as e:
    print("Could not open Safari. Error:", e)