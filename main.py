# Importing local module
from universalAnalytics import *


# Google Analytics Parameters
view_id = ''

# Dimensions and Metrics
dimensions = ['ga:pagePath', 'ga:pageTitle', 'ga:date', 'ga:region', 'ga:city']
metrics = ['ga:users', 'ga:pageviews']

# Stand and End dates of report
start_date = '2023-05-01'
end_date = '2023-05-31'

# Snowflake Parameters for actual data
database = ''
schema = ''
table = ''

# Snowflake Parameters of logging
database_log = ''
schema_log = ''
table_log = ''

if __name__ == '__main__':
    rows = getReportInSnowflake(view_id, 
                                dimensions, 
                                metrics, 
                                start_date, 
                                end_date,
                                database,
                                schema,
                                table,
                                database_log,
                                schema_log,
                                table_log)
    print(rows)