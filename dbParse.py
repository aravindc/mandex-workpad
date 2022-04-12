import os
from unicodedata import name
from logzero import logger
import motor.motor_asyncio
import asyncio


def getMongoDb():
    mongodb_user = os.environ.get('MONGODB_USER')
    mongodb_pass = os.environ.get('MONGODB_PASS')
    mongodb_host = os.environ.get('MONGODB_HOST')
    mongodb_name = os.environ.get('MONGODB_NAME')

    mongodb_url = 'mongodb+srv://{}:{}@{}/{}?retryWrites=true&w=majority'\
        .format(mongodb_user, mongodb_pass, mongodb_host, mongodb_name)

    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_url)
    return client.nightscoutdb


async def getRecords():
    outputs = getMongoDb().entries.find({"dateString": {"$gte": "2022-03-01T00:00:00.000Z", "$lt": "2022-03-01T01:00:00.000Z"}})
    async for output in outputs:
        logger.info(output)




if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(getRecords())

