from typing import Collection
import uuid
import pandas as pd
import numpy as np

from src.preProcessing import *
from utilities.mongoDb import *
from utilities.watcher import *
from utilities.globalConfig import DEFAULT_COLUMN, ATTACK_STAGE_LENGTH, MONGO_COLLECTION_DEFAULT, SIMILARITY_THRESHOLD

from datetime import datetime
from numpy.linalg import norm
from sklearn.decomposition import TruncatedSVD

collectionUniquePattern = 'uniquePattern'
attackStageLength = ATTACK_STAGE_LENGTH #in second
defaultColumns = DEFAULT_COLUMN

def missingValue(dataFrame):
    total = dataFrame.isnull().sum().sort_values(ascending = False)
    Percentage = (dataFrame.isnull().sum()/dataFrame.isnull().count()*100).sort_values(ascending = False)
    Dtypes = dataFrame.dtypes
    return pd.concat([total, Percentage,Dtypes], axis=1, keys=['Total', 'Percentage','Dtypes'])

def dimentionalReductor(data):
  netT = data['NetworkTraffic']
  header = data['ActivityHeaders']

  df= pd.DataFrame(netT, columns=header, dtype=float)
  df= transformation(df, False)
  new_df= df.drop(
          ["StartTime","SrcAddr","Dir","DstAddr","Dport","sTos","dTos","Label","ActivityLabel","NetworkActivity"]
          ,axis=1, errors='ignore')
  new_df['Sport'] = new_df['Sport'].replace('',0).fillna(0).astype(int, errors='ignore')
  new_df['Sport'] = new_df['Sport'].apply(str).apply(int, base=16) #handler icmp port
  new_df['DiffWithPreviousAttack'] = new_df['DiffWithPreviousAttack'].fillna(0).apply(str)
  new_df['position'] = df.index
  
  truncatedSVD=TruncatedSVD(1)
  networkId = truncatedSVD.fit_transform(new_df)
  netId = []
  for x in networkId:
    netId.append(x[0])

  return netId

def dimentionalReductionMultiProcess(query, collection):
  ctx='DIMENTIONAL REDUCTION MULTI PROCESS'
  start = watcherStart(ctx)
  #get unscanned sequential activity
  query['isScanned']= False
  pipelineUnscanned = [
    { '$match': query },
    {
      '$lookup':{
        'from':collection+'-network-traffic',
        'localField':'SequentialActivityId',
        'foreignField':'SequentialActivityId',
        'as': 'NetworkDetail'
      }
    },
    { '$unwind': '$NetworkDetail' },
    {
      '$addFields':{
        'NetworkTraffic': '$NetworkDetail.NetworkTraffic',
        'NetworkActivities': '$NetworkDetail.NetworkActivities',
        'ActivityHeaders': '$NetworkDetail.ActivityHeaders'
      }
    },
    { '$project': { 'NetworkDetail': 0 } }
  ]
  manyUnscanned = aggregate(pipelineUnscanned, collection)
  manyUnscanned = map(addNetId, manyUnscanned)
  manyUnscanned = list(manyUnscanned)
  detection_result = []
  res = {}
  for data in manyUnscanned:
    res = {
      'SequentialActivityId': data['SequentialActivityId'],
      'SrcAddr': data['SrcAddr'],
      'DstAddr': data['DstAddr'],
      'NetworkId': data['NetworkId'],
      'FromDatasets': data['FromDatasets'],
      'DatasetsDetails': data['DatasetsDetails'],
      'CreatedAt': data['CreatedAt'],
      'ModifiedAt': datetime.now(),
      'isScanned': True,
      'sources': data['sources'],
      'lastStartTime': data['lastStartTime']
    }
    detection_result.append(res)

  deleteMany(query, collection)
  insertMany(detection_result, collection)
  watcherEnd(ctx, start)

def addNetId(data):
  data['NetworkId'] = dimentionalReductor(data)
  data['isScanned'] = True
  return data

def sequentialActivityMining(dataframe, stringDatasetName, datasetDetail, sources='DATASETS', collectionName=MONGO_COLLECTION_DEFAULT, columns=defaultColumns):
  ctx='SEQUENTIAL ACTIVITY MINING'
  start = watcherStart(ctx)

  sizeDataframe = len(dataframe)
  progress = 0
  listOfSequenceData={}
  repetationOfAttackStages={} #if more than attackStageLength create new attackStages
  sequenceId = ''
  diff = 0
  tempProgress = 0
  loadingChar=[]
  for index, row in dataframe.iterrows():
    netT = [row[x] for x in columns] #stack values in row to array
    del netT[-1] #delete DiffWithPreviousAttack (change it into diff with previous attack with same source and address)
    sequenceIdPrimary = row['SrcAddr']+'-'+row['DstAddr']
    if(sequenceIdPrimary not in repetationOfAttackStages):
      repetationOfAttackStages[sequenceIdPrimary] = 0
      sequenceId = row['SrcAddr']+'-'+row['DstAddr']+'-0'
    else:
      sequenceId = row['SrcAddr']+'-'+row['DstAddr']+'-'+str(repetationOfAttackStages[sequenceIdPrimary])

    if(sequenceId in listOfSequenceData):
      diff = pd.to_datetime(row['StartTime']) - pd.to_datetime(listOfSequenceData[sequenceId]['lastStartTime'])
      netT.append(diff.seconds)
      if(diff.seconds <= attackStageLength):
        listOfSequenceData[sequenceId]['NetworkTraffic'].append(netT)
        listOfSequenceData[sequenceId]['NetworkActivities'].append(row['NetworkActivity'])
        listOfSequenceData[sequenceId]['modifiedAt'] = datetime.now()
        listOfSequenceData[sequenceId]['lastStartTime'] = row['StartTime']
      else:
        repetationOfAttackStages[sequenceIdPrimary] += 1
        sequenceId = row['SrcAddr']+'-'+row['DstAddr']+'-'+str(repetationOfAttackStages[sequenceIdPrimary])
        listOfSequenceData[sequenceId] = {
        'SequentialActivityId':str(uuid.uuid4()),
        'SrcAddr':row['SrcAddr'],
        'DstAddr':row['DstAddr'],
        'NetworkTraffic': [netT],
        'NetworkActivities': [row['NetworkActivity']],
        'NetworkId':[],
        'FromDatasets': stringDatasetName,
        'DatasetsDetails': str(datasetDetail),
        'ActivityHeaders': columns,
        'createdAt': datetime.now(),
        'modifiedAt': datetime.now(),
        'isScanned': False,
        'sources': sources,
        'lastStartTime':row['StartTime']
      }

    else:
      netT.append(0)
      listOfSequenceData[sequenceId] = {
        'SequentialActivityId':str(uuid.uuid4()),
        'SrcAddr':row['SrcAddr'],
        'DstAddr':row['DstAddr'],
        'NetworkTraffic': [netT],
        'NetworkActivities': [row['NetworkActivity']],
        'NetworkId':[],
        'FromDatasets': stringDatasetName,
        'DatasetsDetails': str(datasetDetail),
        'ActivityHeaders': columns,
        'createdAt': datetime.now(),
        'modifiedAt': datetime.now(),
        'isScanned': False,
        'sources': sources,
        'lastStartTime':row['StartTime']
      }

    #log
    rowCount = index+1
    progress = round(rowCount/sizeDataframe*100)
    if(tempProgress != progress):
      loadingChar.append('~')
      tempProgress = progress
      print(''.join(loadingChar)+str(progress)+'% data scanned!', end="\r")

  values = list(listOfSequenceData.values())
  if sources=='DATASETS':
    insertMany(values, collectionName)
  else:
    detection_NetT = [] #new list contain SequentialActivityId, NetworkTraffic
    netT = {}
    detection_result = []
    res = {}
    for data in values:
      netT = {
        'SequentialActivityId': data['SequentialActivityId'],
        'NetworkTraffic': data['NetworkTraffic'],
        'NetworkActivities': data['NetworkActivities'],
        'ActivityHeaders': data['ActivityHeaders'],
        'sources': data['sources'],
      }
      res = {
        'SequentialActivityId': data['SequentialActivityId'],
        'SrcAddr': data['SrcAddr'],
        'DstAddr': data['DstAddr'],
        'NetworkId': data['NetworkId'],
        'FromDatasets': data['FromDatasets'],
        'DatasetsDetails': data['DatasetsDetails'],
        'CreatedAt': data['createdAt'],
        'ModifiedAt': data['modifiedAt'],
        'isScanned': False,
        'sources': data['sources'],
        'lastStartTime': data['lastStartTime']
      }
      detection_NetT.append(netT)
      detection_result.append(res)

    insertMany(detection_NetT, collectionName+'-network-traffic')
    insertMany(detection_result, collectionName)
  watcherEnd(ctx, start)

#deprecated (need analysis)
def sequentialActivityMiningWithMongo(dataframe, stringDatasetName, datasetDetail, sources='DATASETS', collectionName=MONGO_COLLECTION_DEFAULT, columns=defaultColumns):
  ctx='SEQUENTIAL ACTIVITY MINING-WITH MONGO'
  start = watcherStart(ctx)
  
  tempSrcAddr = ''
  tempDstAddr = ''
  tempSequentialActivity = {}
  sizeDataframe = len(dataframe)
  progress = 0
  tempProgress = 0
  loadingChar=[]
  for index, row in dataframe.iterrows():
    netT = [row[x] for x in columns] #stack values in row to array
    if(tempSrcAddr != row['SrcAddr'] or tempDstAddr != row['DstAddr'] or row['DiffWithPreviousAttack'] >= attackStageLength):
      query = {
        'SrcAddr':row['SrcAddr'],
        'DstAddr':row['DstAddr'],
        'FromDatasets': str(stringDatasetName),
        'DatasetsDetails': str(datasetDetail),
        'sources': sources,
      }
      sequentialActivity = findOne(query, collectionName=collectionName)
      #if the sequential activity not exist in Database
      if(sequentialActivity == None or row['DiffWithPreviousAttack'] >= attackStageLength):
        sequentialActivity = {
          'SequentialActivityId':str(uuid.uuid4()),
          'SrcAddr':row['SrcAddr'],
          'DstAddr':row['DstAddr'],
          'NetworkTraffic': [netT],
          'NetworkActivities': [row['NetworkActivity']],
          'NetworkId':[],
          'FromDatasets': stringDatasetName,
          'DatasetsDetails': str(datasetDetail),
          'ActivityHeaders': columns,
          'createdAt': datetime.now(),
          'modifiedAt': datetime.now(),
          'isScanned': False,
          'sources': sources,
        }
        insertOne(sequentialActivity, collectionName=collectionName)

      #if the sequential activity already exist in Database
      else:
        sequentialActivity['NetworkTraffic'].append(netT)
        sequentialActivity['NetworkActivities'].append(row['NetworkActivity'])
        sequentialActivity['modifiedAt'] = datetime.now()
        upsertOne(
          {'SequentialActivityId': sequentialActivity['SequentialActivityId']},
          sequentialActivity, collectionName=collectionName
        )
      tempSequentialActivity = sequentialActivity

    else:
      tempSequentialActivity['NetworkTraffic'].append(netT)
      tempSequentialActivity['NetworkActivities'].append(row['NetworkActivity'])
      tempSequentialActivity['modifiedAt'] = datetime.now()
      upsertOne(
        {'SequentialActivityId': tempSequentialActivity['SequentialActivityId']},
        tempSequentialActivity, collectionName=collectionName
      )

    tempSrcAddr = row['SrcAddr']
    tempDstAddr = row['DstAddr']
    rowCount = index+1
    #log
    progress = round(rowCount/sizeDataframe*100)
    if(tempProgress != progress):
      loadingChar.append('~')
      tempProgress = progress
      print(''.join(loadingChar)+str(progress)+'% data scanned!', end="\r")
  
  watcherEnd(ctx, start)
#deprecated (need analysis)

def sequentialActivityReduction(stringDatasetName, datasetDetail):
  ctx='SEQUENTIAL ACTIVITY REDUCTION'
  start = watcherStart(ctx)
  print('On going for dataset '+stringDatasetName+' scenario'+str(datasetDetail)+'......')
  # reduce the duplicate
  pipeline =[
    {
      '$match': {
        'FromDatasets': stringDatasetName,
        'DatasetsDetails': str(datasetDetail),
      }
    },
    {
      '$group': {
        '_id': {
          'SrcAddr': '$SrcAddr',
          'NetworkActivities': '$NetworkActivities'
        },
        'count': { '$sum': 1 }
      }
    },
    {'$match': {'_id' :{ '$ne' : None } , 'count' : {'$gt': 1} } }, 
    {'$sort': {'count' : -1} },
    {'$project': {'name' : '$_id', '_id' : 0, 'count': '$count'} }
  ]
  result = aggregate(pipeline)

  for x in result:
    updateMany(x['name'],{
      '$set':{
        'isScanned': True,
      }
    })
    uniquePattern = findOne(x['name'])
    scannedPattern = findOne(x['name'], collectionUniquePattern)
    if(scannedPattern == None):
      uniquePattern['NetworkId'] = dimentionalReductor(uniquePattern)
      insertOne(uniquePattern, collectionUniquePattern)
  #end of reduce the duplicate

  #get unscanned sequential activity
  queryUnscanned = {
    'FromDatasets': stringDatasetName,
    'DatasetsDetails': str(datasetDetail),
    'isScanned': False,
  }
  pipelineUnscanned = [{ '$match': queryUnscanned }]
  manyUnscanned = aggregate(pipelineUnscanned)
  manyUnscannedIdentical = aggregate(pipelineUnscanned, collectionUniquePattern)
  if(manyUnscannedIdentical == []):
    manyUnscanned = map(addNetId, manyUnscanned)
    insertMany(manyUnscanned, collectionUniquePattern)
    updateMany(queryUnscanned,{
      '$set':{
        'isScanned': True,
      }
    })
  #end of get unscanned sequential activity
  watcherEnd(ctx, start)

def similarityMeasurement(query, collection):
  collectionReport = 'report'
  listSimilarity=[]
  report = []
  dictOfPattern={}
  pipeline=[{ '$match': query }]
  netTraffics = aggregate(pipeline, collection)
  for activities in netTraffics:
    del activities['_id']
    activity=activities['NetworkId']
    activitiesLen = len(activity)
    similarity = 0

    #check if pattern isalready get before
    if(activitiesLen not in dictOfPattern):
      patternCharacteristicPipeline=[
        {
          '$match':{
            'NetworkId':{ '$size': activitiesLen }
          }
        }
      ]
      samePattern = aggregate(patternCharacteristicPipeline,collectionUniquePattern)
      dictOfPattern[activitiesLen] = samePattern
    else:
      samePattern = dictOfPattern[activitiesLen]

    patternId = ''
    #start Scanning
    for p in samePattern:      
      pattern=p['NetworkId']

      if(activitiesLen == 1):
        tempSimilarity = norm(activity[0]-pattern[0])/(activity[0]+pattern[0])/2 # compute with difference formula
      else:
        tempSimilarity = np.dot(activity,pattern)/(norm(activity)*norm(pattern)) # compute with cosine similarity

      if(tempSimilarity == 1):
        similarity = tempSimilarity
        patternId =p['SequentialActivityId']
        break
      else:
        similarity = tempSimilarity if tempSimilarity > similarity else similarity
        patternId =p['SequentialActivityId']
    
    activities['SimilarityScore'] = similarity
    activities['PatternId'] = patternId
    report.append(activities)

  deleteMany(query, collectionReport) #overwrite same source file report
  insertMany(report, collectionReport)
    # if similarity > SIMILARITY_THRESHOLD:
    #   listSimilarity.append(round(similarity*100))
  
    # print(listSimilarity)