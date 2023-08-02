from frictionless import validate, Schema, fields, describe
from pprint import pprint
import json
import datetime

# Specify the path to your schema file
#schema_file = Schema.from_descriptor('data/schema.json')

#print(schema_file.missing_values)

current_time = datetime.datetime.now()

print ("Time job Started:", current_time)

# Specify the path to your data file
data_file = "data/input/row_validation_5GB.csv"

# Load the custom schema
##with open(schema_file) as file:
##    custom_schema = file.read()



# Validate the data file using the custom schema
report = validate(data_file)

pprint(report)

pprint(report.flatten(["rowNumber","fieldNumber","type"]))

current_time = datetime.datetime.now()

print ("Time job Stopped:", current_time)
