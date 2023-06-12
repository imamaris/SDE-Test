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

# function case 1
def occupation_daily() :
    #load dataset
    df_rooms = pd.read_csv('dataset/table_rooms.csv')
    df_agreement = pd.read_csv('dataset/table_agreement.csv')
    df_buildings = pd.read_csv('dataset/table_buildings.csv')

    # join tables
    df = pd.merge(df_buildings, df_rooms, how = 'left', left_on = 'id', right_on = 'building_id')
    df = pd.merge(df, df_agreement, how = 'left', on = 'building_id')
    
    # change data type
    df['building_live_date'] = pd.to_datetime(df['building_live_date'])
    df['soft_live_date'] = pd.to_datetime(df['soft_live_date'])
    
    # Filtering data by building_live_date <= today
    today = date.today()
    df = df[df['building_live_date'] <= str(today)]

    # create filter base on requirement
    filtered_occupied = [
        (df['room_status'] != 10) & (df['room_status'] != 13) & df['live_date_confirm'] == True
        ]
    
    occupied_values = [1]
    df["occupied"] = np.select(filtered_occupied, occupied_values, default= 0)

    # get calculate occupied rooms
    df_occupied_rooms_temp = df.groupby(["building_id","occupied"], as_index=False)["room_id"].count()
    df_occupied_rooms_temp = df_occupied_rooms_temp.rename(columns = {"room_id" : "occupied_rooms"})
    df = pd.merge(df, df_occupied_rooms_temp, how = 'inner', on = ['building_id','occupied'])

    # get calculate total rooms
    df_total_rooms_temp = df.groupby(["building_id"], as_index=False)["room_id"].count()
    df_total_rooms_temp = df_total_rooms_temp.rename(columns = {"room_id" : "total_rooms_by_building"})
    df = pd.merge(df, df_total_rooms_temp, how = 'inner', on = ['building_id'])

    df = df[df["occupied"] == 1]
    # calculate occupancy rooms
    df["occupancy"] = df["occupied_rooms"] / df["total_rooms_by_building"]

    # set just column to ingest
    df = df[["property_code","rukita_option","occupancy","building_live_date"]]
    #rename base on requirement
    df = df.rename(columns = {'building_live_date' : 'date'})
    # delete duplicates
    df = df.drop_duplicates()
    # setup data type
    df = df.astype({"property_code" : "string", "rukita_option" : "boolean", "occupancy" : "float", "date" : "datetime64[ns]"})
    #order by
    df = df.sort_values(by=["date", "property_code"])
    # make sure data type is correct
    print(df.info(verbose=True))

    #call function ingest BQ
    create_table_and_load_data(df, PROJECT_ID, DATASET_ID, "daily_occupancy", occupancy_schema, "replace", LOCATION)
    print("Data loaded successfully.")

occupation_daily()
print(yaml.__version__)