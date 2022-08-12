import uuid
import time
from mongoDb import *
from datetime import datetime

collectionUniquePattern = 'uniquePattern'
attackStageLength = 60 #in second
defaultColumns = [
	'StartTime',
	'Dur',
	'Proto',
	'SrcAddr',
	'Sport',
	'Dir',
	'DstAddr',
	'Dport',
	'State',
	'sTos',
	'dTos',
	'TotPkts',
	'TotBytes',
	'SrcBytes',
	'Label',
	'ActivityLabel',
	't1',
	'DiffWithPreviousAttack',
	'NetworkActivity'
]

def sequentialActivityMining(dataframe, stringDatasetName, datasetDetail, columns=defaultColumns):
  print('\n==========|\t\t Sequential Activity Mining [ START ] \t\t\t|==========')
  start = time.time()
  tempSrcAddr = ''
  tempDstAddr = ''
  tempSequentialActivity = {}
  for index, row in dataframe.iterrows():
    netT = [row[x] for x in columns] #stack values in row to array
    if(tempSrcAddr != row['SrcAddr'] or tempDstAddr != row['DstAddr'] or row['DiffWithPreviousAttack'] >= attackStageLength):
      query = {
        'SrcAddr':row['SrcAddr'],
        'DstAddr':row['DstAddr'],
        'FromDatasets': str(stringDatasetName),
        'DatasetsDetails': str(datasetDetail),
      }
      sequentialActivity = findOne(query)
      #if the sequential activity not exist in Database
      if(sequentialActivity == None or row['DiffWithPreviousAttack'] >= attackStageLength):
        sequentialActivity = {
          'SequentialActivityId':str(uuid.uuid4()),
          'SrcAddr':row['SrcAddr'],
          'DstAddr':row['DstAddr'],
          'NetworkTraffic': [netT],
          'NetworkActivities': [row['NetworkActivity']],
          'Vectors':[],
          'FromDatasets': stringDatasetName,
          'DatasetsDetails': str(datasetDetail),
          'ActivityHeaders': columns,
          'createdAt': datetime.now(),
          'modifiedAt': datetime.now(),
          'isScanned': False,
        }
        insertOne(sequentialActivity)

      #if the sequential activity already exist in Database
      else:
        sequentialActivity['NetworkTraffic'].append(netT)
        sequentialActivity['NetworkActivities'].append(row['NetworkActivity'])
        sequentialActivity['modifiedAt'] = datetime.now()
        upsertOne(
          {'SequentialActivityId': sequentialActivity['SequentialActivityId']},
          sequentialActivity
        )
      tempSequentialActivity = sequentialActivity

    else:
      tempSequentialActivity['NetworkTraffic'].append(netT)
      tempSequentialActivity['NetworkActivities'].append(row['NetworkActivity'])
      tempSequentialActivity['modifiedAt'] = datetime.now()
      upsertOne(
        {'SequentialActivityId': tempSequentialActivity['SequentialActivityId']},
        tempSequentialActivity
      )

    tempSrcAddr = row['SrcAddr']
    tempDstAddr = row['DstAddr']

  end = time.time()
  processingTime = end - start
  print('\n==========|\t Sequential Activity Mining [ SUCCESS ] '+str(processingTime)+' s \t|==========')

def sequentialActivityReduction(stringDatasetName, datasetDetail):
  print('\n==========|\t\t Sequential Activity Reduction [ START ] \t\t\t|==========')
  start = time.time()
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
    insertMany(manyUnscanned, collectionUniquePattern)
    updateMany(queryUnscanned,{
      '$set':{
        'isScanned': True,
      }
    })
  #end of get unscanned sequential activity
  end = time.time()
  processingTime = end - start
  print('\n==========|\t Sequential Activity Reduction [ SUCCESS ] '+str(processingTime)+' s \t|==========')