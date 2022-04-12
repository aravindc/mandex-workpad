import csv
import json
import pytz
from datetime import datetime


local_tz = pytz.timezone('Europe/London')

def mmolToGrams(value):
    return round(value * 18.02,0)

def localTimeToUTC(row):
    return row['Timestamp']


def stringToDate(value):
    output = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
    newformat = output.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    print(newformat)
    print(pytz.utc.localize(output.strptime('%Y-%m-%dT%H:%M:%S.%fZ'), is_dst=False).timestamp())
    return (output - datetime(1970,1,1)).total_seconds()*1000


def stringToNewDate(value):
    output = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
    return output.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


def createMongoObject(row):
    jsonObj = {
        "sgv": 39 if row['Glucose Value (mmol/L)'] == 'Low' else mmolToGrams(row['Glucose Value (mmol/L)']),
        "date": 0,
        "dateString": stringToNewDate(row['Timestamp (YYYY-MM-DDThh:mm:ss)']),
        "trend": 8,
        "direction": "Flat",
        "device": "share2",
        "type": "sgv",
        "utcOffset": 0,
        "sysTime": ""
    }    
    return jsonObj


with open('clarity_1.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # print(stringToDate(row['Timestamp']), mmolToGrams(float(1.8 if row['Glucose Value (mmol/L)'] == 'Low' else row['Glucose Value (mmol/L)'])))
        print(row)



# 1648243291000
# 1648243291000

# {
#     "_id":{"$oid":"61716efb2067f7f4039891d1"},
#     "sgv":{"$numberInt":"83"},
#  "date":{"$numberDouble":"1.6348237760000E+12"},
#  "dateString":"2021-10-21T13:42:56.000Z",
#  "trend":{"$numberInt":"4"},
#  "direction":"Flat",
#  "device":"share2",
#  "type":"sgv",
#  "utcOffset":{"$numberInt":"0"},
#  "sysTime":"2021-10-21T13:42:56.000Z"
#  }