from pymongo import MongoClient

client = MongoClient('1.222.84.186', 27017)
db = client.get_database('wanted')

def insertDB(collection, chunk):
    db[collection].insert(chunk, check_keys=False)

def readDB(collection):
    return list(db[collection].find())
