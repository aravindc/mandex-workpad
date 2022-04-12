import os
from unicodedata import name
from logzero import logger
import motor.motor_asyncio
import asyncio
import csv
from datetime import datetime, timedelta
import pytz
import json
import io
from bson import json_util
import pandas as pd

try:
    to_unicode = unicode
except NameError:
    to_unicode = str

def getMongoDb():
    mongodb_user = os.environ.get('MONGODB_USER')
    mongodb_pass = os.environ.get('MONGODB_PASS')
    mongodb_host = os.environ.get('MONGODB_HOST')
    mongodb_name = os.environ.get('MONGODB_NAME')

    mongodb_url = 'mongodb+srv://{}:{}@{}/{}?retryWrites=true&w=majority'\
        .format(mongodb_user, mongodb_pass, mongodb_host, mongodb_name)

    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_url)
    return client.nightscoutdb

def is_dst(when):
    '''Given the name of Timezone will attempt determine if that timezone is in Daylight Saving Time now (DST)'''
    return when.dst() != timedelta(0)

def clarityTimeToUTC(clarityTimeStamp):
    dtValue = datetime.strptime(clarityTimeStamp, '%Y-%m-%dT%H:%M:%S')
    utcTimeStamp = pytz.timezone('Europe/London').localize(dtValue, is_dst=is_dst(dtValue)).timestamp()
    return utcTimeStamp*1000

def getStartEndFromCsv(csvFileName):
    with open(csvFileName, 'r') as inputFile:
        reader = csv.DictReader(inputFile)
        minDate=""
        maxDate=""
        currDate=""
        i = 0
        for row in reader:
            currDate = row['Timestamp (YYYY-MM-DDThh:mm:ss)']
            print('Working on : {}'.format(currDate))
            if minDate == "":
                minDate = row['Timestamp (YYYY-MM-DDThh:mm:ss)']
            if maxDate == "":
                maxDate = row['Timestamp (YYYY-MM-DDThh:mm:ss)']
            if clarityTimeToUTC(currDate) < clarityTimeToUTC(minDate):
                minDate = row['Timestamp (YYYY-MM-DDThh:mm:ss)']
            if clarityTimeToUTC(currDate) > clarityTimeToUTC(maxDate):
                maxDate = row['Timestamp (YYYY-MM-DDThh:mm:ss)']
            # i = i + 1
            # if i == 10:
            #     break
        return clarityTimeToUTC(minDate),clarityTimeToUTC(maxDate)

def parse_json(data):
    return json.loads(json_util.dumps(data))


async def getRecords(minDate, maxDate):
    outputs = getMongoDb().entries.find({"date": {"$gte": minDate, "$lte": maxDate}})
    jsonArr = []
    async for output in outputs:
        output["_id"] = str(output["_id"])
        jsonArr.append(output)
    return jsonArr


def jsonToCsv(jsonFile):
    with open(jsonFile, encoding='utf-8') as inputJsonFile:
        # df = pd.read_json(inputJsonFile)
        jsonArr = json.loads(inputJsonFile.read())
    # df.to_csv('mongoData.csv', encoding='utf-8', index=False)
    count = 0
    with open('mongoData.csv', 'w') as csvOutput:
        csv_writer = csv.writer(csvOutput)
        for jsonObj in jsonArr:
            if count == 0:
                header = jsonObj.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(jsonObj.values())
            logger.info(jsonObj)


if __name__ == '__main__':
    # minD, maxD = getStartEndFromCsv('clarity.csv')
    # logger.info("{} - {}".format(minD, maxD))
    # loop = asyncio.get_event_loop()
    # newArr = loop.run_until_complete(getRecords(minD, maxD))
    # with open('mongoData.json', 'w') as outFile:
    #     json.dump(newArr, outFile, indent=4)
    jsonToCsv('mongoData.json')
