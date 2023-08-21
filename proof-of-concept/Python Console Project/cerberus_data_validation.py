
import frictionless 
from frictionless import validate
from pprint import pprint
from cerberus import Validator 
import pandas as pd
import numpy as np
import csv
from csv import DictReader
import datetime


def validate_dict(data, schema):
    # Create a Cerberus validator instance
    validator = Validator(schema)

    # Validate each row in the data dictionary
    for i, row in enumerate(data, start=1):
        if not validator.validate(row):
            # Print validation errors for each row
            print(f"Validation errors in row {i}: {validator.errors}")
            
        if row.get('Ordering_provider_state') == "" and row.get('Ordering_facility_state') == "":
            print(f"Validation Error in row {i} : columnsOrdering_provider_state and ordering_facility_state are empty")

    if validator.errors:
        print("File and row level validation failed!")
    else:
        print("Dictionary validation successful!")
        

current_time = datetime.datetime.now()

print ("Time job started:", current_time)
        
# Define the schema
schema = {
        'Patient_ID': {'type': 'integer'},
        'Speciman_collection_date_time': {'type': 'integer'},
        'Test_result_status': {'type': 'string', 'required': True, 'allowed': ['Final','Corrected']},
        'patient_state': {'type': 'string','required': True,'nullable': False,'empty': False },
        'Ordering_provider_state': {'type': 'string', 'required': False,'nullable': True,'empty': True },
        'Ordering_facility_state': {'type': 'string', 'required': False,'nullable': True,'empty': True},
        'csv_file_version_no': {'type': 'string', 'required': True, 'allowed': ['V2020-04-18']}
        }

#print(df_dict)

filename = "data/input/row_validation_5GB.csv"
#report = validate(filename)
#pprint(report.flatten(["rowNumber", "fieldNumber", "type"]))

#print(f"Validation Report for file {filename}")

#if (report.flatten(["rowNumber", "fieldNumber", "type"])) == []:
#    print("Generic CSV Validation is successful")

df = pd.read_csv(filename, keep_default_na = False)
    
if df.empty:
    print("File Validation failed: File is empty except header row")
else: 
    
#    df["Patient_ID"] = df["Patient_ID"].astype(int)
#    df["Speciman_collection_date_time"] = df["Speciman_collection_date_time"].astype("int")
        
        
        #df = df.fillna('')
            
    data_dict = df.to_dict(orient='records')
        
        ##pprint(data_dict)
        
    validate_dict(data_dict, schema)
    
#else :
#    print ("File Read error")
    
current_time = datetime.datetime.now()

print ("Time job Stopped:", current_time)