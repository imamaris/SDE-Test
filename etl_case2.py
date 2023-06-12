import os
import sys
import pandas as pd
import numpy as np
import logging
import yaml
from flask import jsonify, make_response
from dotenv import load_dotenv
from datetime import date
# from google.oauth2 import service_account
from script.bigquery_pipeline import create_table_and_load_data

load_dotenv()

logger = logging.getLogger(__name__)

# set variable get date today running this script
today = date.today()

with open('schema/daily_occupancy.yml', 'r') as file:
    occupancy_schema = yaml.safe_load(file)

# set environment your data warehouse
PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_ID = os.getenv('DATASET_ID')
LOCATION = os.getenv('LOCATION')

# cleansing data column
def rename_columns(df):
    columns = {} 
    for col in df.columns:
        columns[col] = col.lower().replace(' ', '_')
    df.rename(columns=columns, inplace=True)
    return df

# function case 2
def leads_daily() :
    #load dataset
    df_leads = pd.read_csv('dataset/leads_data.csv')
    df_signing_data = pd.read_csv('dataset/signing_data.csv')
    df_traffic = pd.read_csv('dataset/traffic.csv')

    # rename columns
    df_leads = rename_columns(df_leads)
    df_signing_data = rename_columns(df_signing_data)
    df_traffic = rename_columns(df_traffic)
    
    # filtering "Only order with “Full Payment” and “Only Deposit” order status that will be counted as signing"
    df_signing_data["signing"] = np.where((df_signing_data["order_status"] == "Full Payment") | (df_signing_data["order_status"] == "Only Deposit"), 1, 0)

    # filtering "Signed Date shouldn’t be after Check In Date, if this happen Signed Date will use Check In Date"
    df_signing_data["final_date"] = np.where(df_signing_data["signed_date"] >=  df_signing_data["check_in_date"], df_signing_data["check_in_date"], df_signing_data["signed_date"])
    
    # join-date leads and signing_data
    df = pd.merge(df_leads, df_signing_data, how = "left", left_on = ['first_contact','email','phone'], right_on = ['final_date','email','phone'])
    # convert date traffic
    df_traffic['date'] = pd.to_datetime(df_traffic['date'])

    # generate final data
    final_df = pd.DataFrame()
    total_signing = df.groupby('final_date', as_index=False)["signing"].count()
    total_signing = total_signing.rename(columns = {"signing" : "number_of_signings"})

    total_leads = df.groupby('first_contact', as_index=False)["email"].count()
    total_leads = total_leads.rename(columns = {"email" : "number_of_leads"}) 

    final_df = pd.merge(total_signing, total_leads, how="right", left_on= "final_date", right_on="first_contact")

    # cleansing final data (exploratory data analyst)
    final_df['first_contact'] = pd.to_datetime(final_df['first_contact'])
    final_df["number_of_signings"] = final_df["number_of_signings"].fillna(0)
    final_df = final_df.rename(columns = {"first_contact" : "date"}) 
    final_df = final_df[['date','number_of_leads','number_of_signings']]
        
    final_df = pd.merge(final_df, df_traffic, how = "left", left_on= "date", right_on="date")
    final_df = final_df.rename(columns= {"views" : "number_of_traffic"})
    final_df = final_df[["number_of_traffic", "number_of_leads", "number_of_signings", "date"]]
    final_df = final_df[final_df['number_of_traffic'].notnull()]
    final_df = final_df.sort_index()
    
    #call function ingest BQ
    create_table_and_load_data(final_df, PROJECT_ID, DATASET_ID, "conversion_leads", occupancy_schema, "replace", LOCATION)
    print("Data loaded successfully.")

leads_daily()