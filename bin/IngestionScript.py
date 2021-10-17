# Script V6
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
import os, sys, json
from google.oauth2 import service_account
#sc

project_id = 'covid-africa-analysis'
dataset_id = 'covidAfrica_stg'
table_name = 'stg_covidAfrica'
file_name = 'covid_africa'

table_id = project_id+"."+dataset_id+"."+table_name

gcp_json_credentials_dict = json.loads(os.environ.get('SERVICE_ACCOUNT'))
credentials = service_account.Credentials.from_service_account_info(gcp_json_credentials_dict)

storage_client = storage.Client(project=gcp_json_credentials_dict['project_id'], credentials=credentials)
client  = bigquery.Client(project=gcp_json_credentials_dict['project_id'], credentials=credentials)

def execute_ingestionLoadJob(table_id):
    table_check = doesTableExist(table_id)
    if (table_check == False):
        createTable(table_id)
        job_config = setupLoadJobConfig()
        return f'Function Execution Completed'
    else:
        print('Table already exists.')
        
def setupLoadJobConfig():
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.skip_leading_rows=1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format=bigquery.SourceFormat.CSV
    return job_config


# A function to check whether a table exists.
def doesTableExist(table_id):
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False

def createTable(table_id):
    try:
        table = bigquery.Table(table_id)

        uri = "gs://covid_africa/covid_africa.csv"
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))

        print("Created table")
    except Exception as e:
        print("Could not create partition table due to " + str(e))

if __name__ == '__main__':

    job_config = setupLoadJobConfig()
    execute_ingestionLoadJob(table_id)