import os
import csv
import json
import pytz
from logzero import logger
import motor.motor_asyncio
import asyncio
from datetime import datetime, timedelta


local_tz = pytz.timezone("Europe/London")


try:
    to_unicode = unicode
except NameError:
    to_unicode = str


def getMongoReadDb():
    mongodb_user = os.environ.get("MONGODB_USER")
    mongodb_pass = os.environ.get("MONGODB_PASS")
    mongodb_host = os.environ.get("MONGODB_HOST")
    mongodb_name = os.environ.get("MONGODB_NAME")
    mongodb_url = "mongodb+srv://{}:{}@{}/{}?retryWrites=true&w=majority".format(
        mongodb_user, mongodb_pass, mongodb_host, mongodb_name
    )
    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_url)
    return client.nightscoutdb


def getMongoWriteDb():
    mongodb_user = os.environ.get("MONGODB_WRITE_USER")
    mongodb_pass = os.environ.get("MONGODB_WRITE_PASS")
    mongodb_host = os.environ.get("MONGODB_HOST")
    mongodb_name = os.environ.get("MONGODB_NAME")
    mongodb_url = "mongodb+srv://{}:{}@{}/{}?retryWrites=true&w=majority".format(
        mongodb_user, mongodb_pass, mongodb_host, mongodb_name
    )
    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_url)
    return client.nightscoutdb


async def getRecordsByMinMaxDate(minDate, maxDate):
    outputs = getMongoReadDb().entries.find(
        {"date": {"$gte": minDate, "$lte": maxDate}}
    )
    jsonArr = []
    async for output in outputs:
        output["_id"] = str(output["_id"])
        jsonArr.append(output)
    return jsonArr


async def getRecordByDate(inputDate):
    outputs = getMongoReadDb().entries.find({"date": {"$eq": inputDate}})
    logger.info(outputs)
    async for output in outputs:
        logger.info(output)


def getStartEndFromCsv(inputClarityFileName):
    with open(inputClarityFileName + ".csv", "r") as inputFile:
        reader = csv.DictReader(inputFile)
        minDate = ""
        maxDate = ""
        currDate = ""
        i = 0
        for row in reader:
            currDate = row["Timestamp (YYYY-MM-DDThh:mm:ss)"]
            # print('Working on : {}'.format(currDate))
            if minDate == "":
                minDate = row["Timestamp (YYYY-MM-DDThh:mm:ss)"]
            if maxDate == "":
                maxDate = row["Timestamp (YYYY-MM-DDThh:mm:ss)"]
            if clarityTimeToUTC(currDate) < clarityTimeToUTC(minDate):
                minDate = row["Timestamp (YYYY-MM-DDThh:mm:ss)"]
            if clarityTimeToUTC(currDate) > clarityTimeToUTC(maxDate):
                maxDate = row["Timestamp (YYYY-MM-DDThh:mm:ss)"]
            # i = i + 1
            # if i == 10:
            #     break
        return clarityTimeToUTC(minDate), clarityTimeToUTC(maxDate)


def jsonToCsv(jsonArr, inputClarityFileName):
    # with open(jsonFile, encoding='utf-8') as inputJsonFile:
    #     # df = pd.read_json(inputJsonFile)
    #     jsonArr = json.loads(inputJsonFile.read())
    # # df.to_csv('mongoData.csv', encoding='utf-8', index=False)
    count = 0
    with open(inputClarityFileName + "-mongo.csv", "w") as csvOutput:
        csv_writer = csv.writer(csvOutput)
        for jsonObj in jsonArr:
            if count == 0:
                header = jsonObj.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(jsonObj.values())
            logger.info(jsonObj)


def mmolToGrams(value):
    return int(round(value * 18.0, 0))


def stringToNewDate(value):
    output = datetime.fromtimestamp(value / 1000, pytz.utc)
    return output.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def is_dst(when):
    """Given the name of Timezone will attempt determine if that timezone is in Daylight Saving Time now (DST)"""
    return when.dst() != timedelta(0)


def clarityTimeToUTC(clarityTimeStamp):
    dtValue = datetime.strptime(clarityTimeStamp, "%Y-%m-%dT%H:%M:%S")
    utcTimeStamp = (
        pytz.timezone("Europe/London")
        .localize(dtValue, is_dst=is_dst(dtValue))
        .timestamp()
    )
    return utcTimeStamp * 1000


def createMongoObject(row):
    # sgv = mmolToGrams(float(row['Glucose Value (mmol/L)']))
    # dateValue = clarityTimeToUTC(row['Timestamp (YYYY-MM-DDThh:mm:ss)'])
    sgv = mmolToGrams(float(row[7]))
    dateValue = clarityTimeToUTC(row[1])
    dateStringValue = stringToNewDate(dateValue)
    jsonObj = {
        # "sgv": 39 if row['Glucose Value (mmol/L)'] == 'Low' else sgv,
        "sgv": 39 if row[7] == "Low" else sgv,
        "date": dateValue,
        "dateString": dateStringValue,
        "trend": 8,
        "direction": "NOT COMPUTABLE",
        "device": "share2",
        "type": "sgv",
        "utcOffset": 0,
        "sysTime": dateStringValue,
    }
    return jsonObj


def generateUtFile(inputClarityFileName):
    with open(inputClarityFileName + ".csv", "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        logger.info(header)
        with open(inputClarityFileName + "-utc.csv", "w") as outputFile:
            output_writer = csv.writer(outputFile)
            header.append("TimeStampUtc")
            output_writer.writerow(header)
            for row in reader:
                # row[14] = stringToNewDate(clarityTimeToUTC(row[1]))
                row.append(stringToNewDate(clarityTimeToUTC(row[1])))
                try:
                    # logger.info(createMongoObject(row))
                    logger.info(row)
                    output_writer.writerow(row)
                except Exception as e:
                    logger.info(row)
                    logger.error(e)


def generateMissingData(inputClarityFileName):
    outputFile = open(inputClarityFileName + "-missing.csv", "w")
    output_writer = csv.writer(outputFile)
    m_found = False
    with open(inputClarityFileName + "-utc.csv", "r") as clarityFile:
        clarity_reader = csv.reader(clarityFile)
        __ = next(clarity_reader)  # Skip header
        for c_row in clarity_reader:
            c_row[7] = 2.17 if c_row[7] == "Low" else c_row[7]
            with open(inputClarityFileName + "-mongo.csv", "r") as mongoFile:
                mongo_reader = csv.reader(mongoFile)
                __ = next(mongo_reader)
                for m_row in mongo_reader:
                    if c_row[14] == m_row[3]:
                        # logger.info('{} - {} - {} - {} - {}'.format(c_row[14],c_row[7],m_row[1],float(m_row[1])/float(c_row[7]),round(float(c_row[7])*18,0)))
                        m_found = True
                        break
            if m_found == False:
                logger.info(c_row)
                output_writer.writerow(c_row)
            else:
                m_found = False
    outputFile.close()


def insertMongoMissingData(inputClarityFileName):
    with open(inputClarityFileName + "-missing.csv", "r") as inputFile:
        reader = csv.reader(inputFile)
        __ = next(reader)
        for row in reader:
            mongoObj = createMongoObject(row)
            logger.info(mongoObj)
            loop = asyncio.get_event_loop()
            recExists = loop.run_until_complete(getRecordByDate(mongoObj["date"]))
            if recExists is None:
                loop.run_until_complete(insertMissingMongo(createMongoObject(row)))
            else:
                for output in recExists:
                    print(output)


async def insertMissingMongo(inputRow):
    rec_id = await getMongoWriteDb().entries.insert_one(inputRow)
    logger.info("Record inserted with ID: {}".format(rec_id))


if __name__ == "__main__":
    inputClarityFile = "clarity_20220504"
    minD, maxD = getStartEndFromCsv(inputClarityFile)
    logger.info("{} - {}".format(minD, maxD))
    loop = asyncio.get_event_loop()
    newArr = loop.run_until_complete(getRecordsByMinMaxDate(minD, maxD))
    jsonToCsv(newArr, inputClarityFile)
    generateUtFile(inputClarityFile)
    generateMissingData(inputClarityFile)
    insertMongoMissingData(inputClarityFile)
