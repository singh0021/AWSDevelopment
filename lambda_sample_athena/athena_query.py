from datetime import datetime, timedelta, timezone
import time
import json
import logging
import boto3

athena_client = boto3.client('athena')


def run_short_query(QueryExecutionContext, ResultConfiguration):
    query = """Insert Query String"""

    res = athena_client.start_query_execution(QueryString=query, QueryExecutionContext=QueryExecutionContext,
                                              ResultConfiguration=ResultConfiguration)

    query_id = res['QueryExecutionId']


    res = athena_client.get_query_execution(QueryExecutionId=query_id)
    if res['QueryExecution']['Status']['State'] == 'SUCCEEDED':

        res = athena_client.get_query_execution(QueryExecutionId=query_id)

    return res


def lambda_handler(event, context):
    dt = datetime.now()

    QueryExecutionContext = {
        'Database': 'db_name'
    }
    ResultConfiguration = {
        'OutputLocation': dt.strftime('s3://location'),
    }


    result = run_short_query(QueryExecutionContext, ResultConfiguration)

    print("Scheduled Athena-Query - Query Execution Id: {}".format(result['QueryExecution']['QueryExecutionId']))

    return {
        'statusCode': 200,
        'body': "Scheduled Athena-Query - Query Execution Id: {}".format(result['QueryExecution']['QueryExecutionId'])
    }