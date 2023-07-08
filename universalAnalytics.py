import pytz
import json
import datetime
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import to_date
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

scopes = ['https://www.googleapis.com/auth/analytics.readonly']
key_file_location = 'service-account-key.json'

# Creating UA Authentication Object
credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location, scopes)
analytics = build('analyticsreporting', 'v4', credentials=credentials)

with open('snowflake.json', 'r') as d:
    connection_parameters = json.load(d)
session = Session.builder.configs(connection_parameters).create()

def responseToDataframe(response: dict ) -> pd.DataFrame:
    """
    Converts a response dictionary to a Snowpark DataFrame.

    Args:
        response (dict): The response dictionary containing the data.

    Returns:
        sesison.DataFrame: A Snowpark DataFrame containing the extracted data.

    Raises:
        KeyError: If the required keys are not present in the response dictionary.
    """

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

    return session.createDataFrame(data, schema = dimensions + metrics)

def dataframeToSnowflake(df, 
                         database:str, 
                         schema:str, 
                         table:str) -> int:
    """
    Writes a Snowpark DataFrame to Snowflake database.

    Args:
        df (session.DataFrame): The Snwopark DataFrame to be written.
        database (str): The name of the Snowflake database.
        schema (str): The name of the schema within the database.
        table (str): The name of the table to write the data into.

    Returns:
        int: The number of rows written to Snowflake.
    """
    df.write.mode("append").save_as_table([database, schema, table])
    return df.count()

def getReportInSnowflake(view_id:str, 
                    dimensions:list, 
                    metrics:list, 
                    start_date:str, 
                    end_date:str,
                    database:str,
                    schema:str,
                    table:str,
                    database_log:str,
                    schema_log:str,
                    table_log:str,
                    page_size:int=100000,
                    page_token:str=None,
                    ) -> int:
    
    """

    Retrieves a report from Google Analytics Reporting API and transfers it to Snowflake database.

    Args:
        view_id (str): The ID of the Google Analytics view.
        dimensions (list): A list of dimensions to include in the report.
        metrics (list): A list of metrics to include in the report.
        start_date (str): The start date of the report in the format 'YYYY-MM-DD'.
        end_date (str): The end date of the report in the format 'YYYY-MM-DD'.
        database (str): Snowflake database in which data is going to be.
        schema (str): Snowflake schema where table is going to be.
        table (str): Snowflake Table where data in going to be.
        database_log (str): Log Database.
        schema_log (str): Log Schema.
        table_log (str): Log Table.
        page_size (int, optional): The number of rows to retrieve per API call. Defaults to 100000.
        page_token (str, optional): The page token for pagination. Defaults to None. Can be used for debugging.

    Returns:
        int: The total number of rows transferred to Snowflake.

    Example:
        dimensions = ['ga:date', 'ga:source']
        metrics = ['ga:sessions', 'ga:users']
        start_date = '2023-01-01'
        end_date = '2023-01-31'
        database = 'Google'
        schema = 'UA'
        table = 'data'
        database_log = 'Google_logs'
        schema_log = 'Raw'
        table_log = 'Load_logs'
        total_rows = getReportInSnowflake('12345678', dimensions, metrics, start_date, end_date, database, schema, table, database_log, schema_log, table_log)
    """
    page_token = None
    total_rows : int = 0
    api_calls_counter : int = 0
    mets = [{'expression': m} for m in metrics]
    dims = [{'name': d} for d in dimensions]

    current_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    while current_date <= end:
        current_start_date = current_date.strftime('%Y-%m-%d')
        current_end_date = current_date.strftime('%Y-%m-%d')

        while True:
            response = analytics.reports().batchGet(
                body={
                    'reportRequests': [
                    {
                    'viewId': view_id,
                    'dateRanges': [{'startDate': current_start_date, 'endDate': current_end_date}],
                    'metrics': mets,
                    'dimensions': dims,
                    'pageSize': page_size,
                    'pageToken': page_token
                    }]
                }
                ).execute()
            
            if 'rows' in response['reports'][0]['data']:
                # Converting response to snowpark dataframe
                dataframe = responseToDataframe(response)

                # grab datatypes from response in a dictionary
                dataTypes = {value['name']: value['type'] for value in response["reports"][0]["columnHeader"]["metricHeader"]["metricHeaderEntries"]}

                # convert all metrics to double
                for key in dataTypes.keys():
                    dataframe = dataframe.withColumn(key, dataframe[key].cast('double'))
                
                # adding loadtimestamp in dataframe
                current_timestamp = datetime.datetime.now(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
                dataframe = dataframe.withColumn('LOADTIMESTAMP', lit(current_timestamp))

                # typecasting date and loadtimestamp column
                dataframe = dataframe.withColumn("ga:date", to_date(col("ga:date"), 'YYYYMMDD'))
                dataframe = dataframe.withColumn('LOADTIMESTAMP', dataframe["LOADTIMESTAMP"].cast('timestamp'))

                # Trasnfering dataframe to snowflake
                rows = dataframeToSnowflake(dataframe, database, schema, table)

                total_rows = total_rows + rows
                api_calls_counter += 1

                # Check if the next page token is present
                if 'nextPageToken' in response['reports'][0]:
                    page_token = response['reports'][0]['nextPageToken']
                else:
                    break  # No more pages, exit the loop

            else:
                break

        current_date += datetime.timedelta(days=1)


    # Creating Log entries
    logEntry = {
        'StartDate' : start_date,
        'EndDate' : end_date,
        'RecordsSent' : total_rows,
        'TableName' : table,
        'APICalls' : api_calls_counter
    }

    logs = [logEntry]
    
    # Creating dataframe of logs
    loggingDataframe = session.createDataFrame(logs)

    # adding loadtimestamp in dataframe
    current_timestamp = datetime.datetime.now(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
    loggingDataframe = loggingDataframe.withColumn('LOADTIMESTAMP', lit(current_timestamp))
    loggingDataframe = loggingDataframe.withColumn('LOADTIMESTAMP', dataframe["LOADTIMESTAMP"].cast('timestamp'))

    # typecasting start and end date columns as date
    loggingDataframe = loggingDataframe.withColumn("STARTDATE", to_date(col("STARTDATE"), 'YYYY-MM-DD'))
    loggingDataframe = loggingDataframe.withColumn("ENDDATE", to_date(col("ENDDATE"), 'YYYY-MM-DD'))

    # Transfering dataframe to snowflake
    dataframeToSnowflake(loggingDataframe, database_log, schema_log, table_log)

    return total_rows