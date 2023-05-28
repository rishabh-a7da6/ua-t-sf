# Importing Libraries
import os
import json
import pandas as pd
import snowflake.connector as snow
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from snowflake.connector.pandas_tools import write_pandas

# Universal Analytics Parameters
scopes = ['https://www.googleapis.com/auth/analytics.readonly']
key_file_location = 'service-account-key.json'

view_id = ''

dimensions = ['ga:pagePath', 'ga:pageTitle']
metrics = ['ga:pageviews', 'ga:uniquePageviews', 'ga:avgTimeOnPage']
start_date = '2023-05-01'
end_date = 'yesterday'

# Creating UA Authentication Object
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, scopes)
analytics = build('analyticsreporting', 'v4', credentials=credentials)

# Method to get report
def get_report(view_id:str, 
               dimensions:list, 
               metrics:list, 
               start_date:str, 
               end_date:str):
    """Retrieves a report from Google Analytics Reporting API

    Parameters:
        view_id (str): The view ID of the Google Analytics account.
        dimensions (list): A list of dimensions to include in the report.
        metrics (list): A list of metrics to include in the report.
        start_date (str): The start date of the report in 'YYYY-MM-DD' format.
        end_date (str): The end date of the report in 'YYYY-MM-DD' format.

    Returns:
        dict: A dictionary containing the retrieved report data.

    Raises:
        googleapiclient.errors.HttpError: If an error occurs during the API request.

    Note:
        - The dimensions and metrics should be specified as valid expressions for the Google Analytics Reporting API.
        - The start_date and end_date should be valid dates in the 'YYYY-MM-DD' format.

    Example Usage:
        response = get_report('12345678', ['ga:pagePath', 'ga:pageTitle'], ['ga:pageviews', 'ga:uniquePageviews'],
                              '2023-01-01', '2023-12-31')
    """
    mets = [{'expression': m} for m in metrics]
    dims = [{'name': d} for d in dimensions]
    
    return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': view_id,
          'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
          'metrics': mets,
          'dimensions': dims
        }]
      }
    ).execute()

# Getting actual report data
response = get_report(view_id, dimensions, metrics, start_date, end_date)

# Converting response to Dataframe
# Extracting column names
dimensions = response['reports'][0]['columnHeader']['dimensions']
metrics = [entry['name'] for entry in response['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']]

# Extract the data rows
rows = response['reports'][0]['data']['rows']
data = []
for row in rows:
    dimensions_values = row['dimensions']
    metrics_values = [value for metric in row['metrics'] for value in metric['values']]
    data.append(dimensions_values + metrics_values)

# Final dataframe
dataframe = pd.DataFrame(data, columns = dimensions + metrics)

# Snowflke configuration
database = ''
schema = ''
table = ''

# Connection Method
def snowflake_connection(file_path):
    """Establishes a connection to Snowflake using the credentials read from a JSON file.

    Parameters:
        file_path (str): The path to the JSON file containing the Snowflake credentials.

    Returns:
        snowflake.connector.connection.SnowflakeConnection: The Snowflake connection object.
    """
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    user = data['user']
    password = data['password']
    account = data['account']
    role = data['role']
    warehouse = data['warehouse']
    database = data['database']
    schema = data['schema']

    conn = snow.connect(
        user=user,
        password=password,
        account=account,
        role=role,
        warehouse=warehouse,
        database=database.upper(),
        schema=schema.upper()
    )

    return conn

# Dataframe to Snowflake
success, nchunks, nrows, out = write_pandas(snowflake_connection('snowflake.json'),
                                            dataframe,
                                            table.upper(),
                                            database.upper(),
                                            schema.upper(),
                                            auto_create_table = True
                                           )

# Printing number of transfered rows
print(f"Number of rows transferred : {nrows}")