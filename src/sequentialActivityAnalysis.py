import uuid
import pandas as pd

from src.preProcessing import *
from utilities.mongoDb import *
from utilities.watcher import *
from utilities.globalConfig import DEFAULT_COLUMN, ATTACK_STAGE_LENGTH, MONGO_COLLECTION_DEFAULT

from datetime import datetime
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
          ["StartTime","Dir","DstAddr","Dport","sTos","dTos","Label","ActivityLabel","NetworkActivity"]
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

  print(netId)
  return netId

def dimentionalReductionMultiProcess(query, collection):
  #get unscanned sequential activity
  query['isScanned']= False
  pipelineUnscanned = [{ '$match': query }]
  manyUnscanned = aggregate(pipelineUnscanned, collection)
  manyUnscanned = map(addNetId, manyUnscanned)
  deleteMany(query, collection)
  insertMany(manyUnscanned, collection)

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

  values = listOfSequenceData.values()
  insertMany(values, collectionName)
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
