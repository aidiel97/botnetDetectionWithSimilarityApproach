import pymongo

from utilities.globalConfig import MONGO_URL, MONGO_DATABASE, MONGO_COLLECTION_DEFAULT

mongoClient = pymongo.MongoClient(MONGO_URL) #better user .env
database = mongoClient[MONGO_DATABASE] #better user .env
defaultCollection = MONGO_COLLECTION_DEFAULT #better user .env

#commmand
def insertOne(dictData, collectionName=defaultCollection):
  collection = database[collectionName]
  row = collection.insert_one(dictData)
  print("success insert with id: "+str(row.inserted_id))

def insertMany(listData, collectionName=defaultCollection):
  collection = database[collectionName]
  row = collection.insert_many(listData)
  print("success insert with id: "+str(row.inserted_ids))

def upsertOne(query, record, collectionName=defaultCollection):
  collection = database[collectionName]
  collection.replace_one(query, record, upsert=True)
  print("Success upsert ", str(query))

def updateMany(query, record, collectionName=defaultCollection):
  collection = database[collectionName]
  collection.update_many(query, record)
  print("Success update ", str(query))

def deleteOne(query, collectionName=defaultCollection):
  collection = database[collectionName]
  collection.delete_one(query)
  print("Success delete ", str(query))

#query
def findOne(query={}, collectionName=defaultCollection):
  collection = database[collectionName]
  doc = collection.find_one(query)

  return doc

def aggregate(pipeline, collectionName=defaultCollection):
  result = []
  collection = database[collectionName]
  doc = collection.aggregate(pipeline)

  for record in doc:
    result.append(record)

  return result
