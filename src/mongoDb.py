import pymongo

mongoClient = pymongo.MongoClient("mongodb://localhost:27017/") #better user .env
database = mongoClient["knowledge-based"] #better user .env
defaultCollection = "sequentialActivities" #better user .env

#commmand
def insertOne(dictData, collectionName=defaultCollection):
  collection = database[collectionName]
  row = collection.insert_one(dictData)
  print("success insert with id: "+str(row.inserted_id))

def insertMany(listData, collectionName=defaultCollection):
  collection = database[collectionName]
  row = collection.insert_Many(listData)
  print("success insert with id: "+str(row.inserted_ids))

def upsertOne(query, record, collectionName=defaultCollection):
  collection = database[collectionName]
  collection.replace_one(query, record, upsert=True)
  print("Success upsert ", str(query))

#query
def findOne(query={}):
  collection = database[defaultCollection]
  doc = collection.find_one(query)

  return doc