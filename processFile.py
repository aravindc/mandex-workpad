import csv
import json
from datetime import datetime, timedelta
import pytz
from logzero import logger


local_tz = pytz.timezone('Europe/London')

def mmolToGrams(value):
    return int(round(value * 18.02,0))

def stringToNewDate(value):
    output = datetime.fromtimestamp(value/1000, pytz.utc)
    return output.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

def is_dst(when):
    '''Given the name of Timezone will attempt determine if that timezone is in Daylight Saving Time now (DST)'''
    return when.dst() != timedelta(0)

def clarityTimeToUTC(clarityTimeStamp):
    dtValue = datetime.strptime(clarityTimeStamp, '%Y-%m-%dT%H:%M:%S')
    utcTimeStamp = pytz.timezone('Europe/London').localize(dtValue, is_dst=is_dst(dtValue)).timestamp()
    return utcTimeStamp*1000

def createMongoObject(row):
    sgv = mmolToGrams(float(row['Glucose Value (mmol/L)']))
    dateValue = clarityTimeToUTC(row['Timestamp (YYYY-MM-DDThh:mm:ss)'])
    dateStringValue = stringToNewDate(dateValue)
    jsonObj = {
        "sgv": 39 if row['Glucose Value (mmol/L)'] == 'Low' else sgv,
        "date": dateValue,
        "dateString": dateStringValue,
        "trend": 8,
        "direction": "Flat",
        "device": "share2",
        "type": "sgv",
        "utcOffset": 0,
        "sysTime": dateStringValue
    }
    return jsonObj


with open('clarity_20220411.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            logger.info(createMongoObject(row))
        except Exception as e:
            logger.info(row)
            logger.error(e)
