import boto3, sys, json, time, uuid, datetime, botocore, re

import traceback


def my_print(msg):
    print("{} {}".format(datetime.datetime.utcnow(), msg))


class AthenaMetrics(object):
    region_name = 'eu-west-1'
    client = boto3.session.Session(region_name=region_name).client('athena')

    s3_folder = None

    def __init__(self, s3_folder):

        self.s3_folder = s3_folder

    def execute_save_s3(self, sql_query, s3_output_folder):

        QueryExecutionId = self.start_query_execution(sql_query=sql_query, s3_output_folder=s3_output_folder)
        result_status_result = self.__wait_for_query_to_complete(QueryExecutionId)
        if result_status_result["SUCCESS"]:
            return True
        else:
            raise Exception(result_status_result)

    def execute_query(self, sql_query, use_cache=False):

        QueryExecutionId = self.start_query_execution(sql_query=sql_query, use_cache=use_cache)
        return self.get_results(QueryExecutionId=QueryExecutionId)

    def start_query_execution(self, sql_query, use_cache=False, s3_output_folder=None):

        outputLocation = 's3://' + self.s3_folder if s3_output_folder is None else s3_output_folder

        response = self.client.start_query_execution(
            QueryString=sql_query,
            ClientRequestToken=str(uuid.uuid4()) if not use_cache else sql_query[:64].ljust(32) + str(hash(sql_query)),
            ResultConfiguration={
                'OutputLocation': outputLocation,
            }
        )
        return response["QueryExecutionId"]

    def get_results(self, QueryExecutionId):

        result_status_result = self.__wait_for_query_to_complete(QueryExecutionId)

        result = None
        if result_status_result["SUCCESS"]:
            paginator = self.client.get_paginator('get_query_results')
            page_response = paginator.paginate(QueryExecutionId=QueryExecutionId)
            # PageResponse Holds 1000 objects at a time and will continue to repeat in chunks of 1000.

            for page_object in page_response:

                if result is None:
                    result = page_object
                    if result_status_result["QUERY_TYPE"] == "SELECT":
                        if len(result["ResultSet"]["Rows"]) > 0:
                            # removes column header from 1st row (Athena returns 1st row as col header)
                            del result["ResultSet"]["Rows"][0]
                else:
                    result["ResultSet"]["Rows"].extend(page_object["ResultSet"]["Rows"])
            result["ResponseMetadata"]["HTTPHeaders"]["content-length"] = None
            return result
        else:
            raise Exception(result_status_result)

    def __wait_for_query_to_complete(self, QueryExecutionId):

        status = "QUEUED"  # assumed
        error_count = 0
        response = None
        while (status in ("QUEUED','RUNNING")):  # can be QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
            try:
                response = self.client.get_query_execution(QueryExecutionId=QueryExecutionId)
                status = response["QueryExecution"]["Status"]["State"]
                # my_print(status)
                time.sleep(0.5)
            except botocore.exceptions.ClientError as ce:

                error_count = error_count + 1
                if (error_count > 3):
                    status = "FAILED"
                    print(str(ce))
                    break  # out of the loop
                if "ExpiredTokenException" in str(ce):
                    self.client = boto3.session.Session(region_name=self.region_name).client('athena')

        if (status == "FAILED" or status == "CANCELLED"):
            # print(response)
            pass

        if response is None:
            return {"SUCCESS": False,
                    "STATUS": status
                , "QUERY_TYPE": response["QueryExecution"]["Query"].strip().upper().split(" ")[0]
                , "QUERY": response["QueryExecution"]["Query"]
                , "StateChangeReason": response["QueryExecution"]["Status"][
                    "StateChangeReason"] if "StateChangeReason" in response["QueryExecution"]["Status"] else None}
        else:
            return {"SUCCESS": True if response["QueryExecution"]["Status"]["State"] == "SUCCEEDED" else False,
                    "STATUS": response["QueryExecution"]["Status"]["State"]
                , "QUERY_TYPE": response["QueryExecution"]["Query"].strip().upper().split(" ")[0]
                , "QUERY": response["QueryExecution"]["Query"]
                , "StateChangeReason": response["QueryExecution"]["Status"][
                    "StateChangeReason"] if "StateChangeReason" in response["QueryExecution"]["Status"] else None}
