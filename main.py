from athena_query import AthenaMetrics
import boto3
import datetime
import sys
import os
import csv, time


def array_chunk(array, chunksize):
    res = []
    lower = 0
    for j in range(lower, len(array)):
        tmp = []
        upper = lower + chunksize
        if (upper >= len(array)):
            upper = len(array)
        for k in range(lower, upper):
            tmp.append(array[k])
        lower = upper
        res.append(tmp)
        if (upper >= len(array)):
            return res


def lambda_handler(event, context):
    folder = os.environ['folder_path']
    bucket = os.environ['bucket']

    bucket_folder = bucket + '/' + folder

    client = boto3.client('athena', region_name='us-east-1')

    ## The Athena query on cloudtrails table to get last days' query IDs
    query_str = """
                Query String
                """

    result = AthenaMetrics(s3_folder=bucket_folder) \
        .execute_query(query_str)

    query_ids = []
    for row in result["ResultSet"]["Rows"]:
        data = row['Data'][0]
        print(row)
        item = data['VarCharValue'].strip('"')
        query_ids.append(item)

    allqueryids = (query_ids[1:])

    j = 0

    for batchqueryids in array_chunk(allqueryids, 50):
        try:
            response = client.batch_get_query_execution(
                QueryExecutionIds=batchqueryids
            )
            athena_metrics = ""
            for row in response["QueryExecutions"]:
                queryid = row['QueryExecutionId']
                querydatabase = "null"
                if 'QueryExecutionContext' in row and 'Database' in row['QueryExecutionContext']:
                    querydatabase = row['QueryExecutionContext']['Database']
                executiontime = "null"
                if 'EngineExecutionTimeInMillis' in row['Statistics']:
                    executiontime = str(row['Statistics']['EngineExecutionTimeInMillis'])
                datascanned = "null"
                if 'DataScannedInBytes' in row['Statistics']:
                    datascanned = str(row['Statistics']['DataScannedInBytes'])
                status = row['Status']['State']
                submissiondatetime = "null"
                if 'SubmissionDateTime' in row['Status']:
                    submissiondatetime = str(row['Status']['SubmissionDateTime'])
                completiondatetime = "null"
                if 'CompletionDateTime' in row['Status']:
                    completiondatetime = str(row['Status']['CompletionDateTime'])
                athena_metrics += ','.join(
                    [queryid, querydatabase, executiontime, datascanned, status, submissiondatetime,
                     completiondatetime]) + '\n'
        except Exception as e:
            print(e)

        filename = "query_result_" + str(j) + ".csv"
        lambda_path = "/tmp/" + filename

        f = open(lambda_path, 'w')
        f.write(athena_metrics)
        f.close()

        s3 = boto3.resource('s3', region_name='us-east-1')

        now = datetime.datetime.now()
        current_year = now.year
        current_month = f"{now.month:02d}"
        current_day = f"{now.day :02d}"

        outfile = 'athena-metrics' + '/' + str(current_year) + '/' + str(current_month) + '/' + str(
            current_day) + '/' + 'query_result_' + str(j) + '.csv'
        s3.meta.client.upload_file(lambda_path, bucket, outfile)

        j = j + 1

        print("Printing Athena Metrics on Console", athena_metrics)


