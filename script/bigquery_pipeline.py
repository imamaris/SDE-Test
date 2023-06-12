from google.oauth2.service_account import Credentials
from google.cloud import bigquery
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# set environment credential
CREDENTIAL_FILE = os.getenv('CREDENTIAL_FILE')

def create_table_and_load_data(df, project, dataset_id, table_id, schema, parameter, location):

    # Load the service account credentials from the JSON file
    credentials = Credentials.from_service_account_file(CREDENTIAL_FILE,)
    destination = f"{dataset_id}.{table_id}"
    
    return df.to_gbq(project_id = project, credentials= credentials,destination_table = destination, table_schema = schema, if_exists = parameter, location = location)
