from msilib.schema import Condition
import pandas as pd
import csv

import src.classificator as cl
import src.dataSplitter as ds
import src.preProcessing as pp
import src.sequentialActivityAnalysis as saa
import utilities.dataLoader as loader
import utilities.menuManagement as menu

from datetime import datetime

from utilities.globalConfig import *
from utilities.watcher import *
from utilities.mongoDb import *

def similarityAnalyst():
	ctx= 'SIMILARITY ANALYSIS'
	start = watcherStart(ctx)
	watcherEnd(ctx, start)

# def recons(dataframe, stringDatasetName, datasetDetail):
def recons():
	ctx = 'TRAFFIC RECON'
	start = watcherStart(ctx)
	print(DEFAULT_COLUMN)
	watcherEnd(ctx, start)

def detectionWithMachineLearning(datasetName, selectedScenario):
  raw_df = loader.loadDataset(datasetName, selectedScenario)
  df = pp.preProcessing(raw_df) #preProcessed dataframe
  train, test = ds.splitTestAllDataframe(df)

  result = cl.classification(df, train, test, selectedScenario, 'randomForest')
  raw_df = raw_df.drop(['ActivityLabel'],axis=1)
  data = raw_df.assign(ActivityLabel=result) #assign new column

  botnet_df = data[data['ActivityLabel'].isin([1])] #create new bot dataframes
  normal_df = data[data['ActivityLabel'].isin([0])] #create new normal dataframes
  botnet_df.reset_index(drop=True, inplace=True) #reset index from parent dataframe
  normal_df.reset_index(drop=True, inplace=True) #reset index from parent dataframe

  return botnet_df, normal_df

def detectionWithLabel(datasetName, selectedScenario): #not intended for anything else except of a knowledgebase
  df = loader.loadDataset(datasetName, selectedScenario)
  df['ActivityLabel'] = df['Label'].str.contains('botnet', case=False, regex=True).astype(int)
  botnet_df = df[df['ActivityLabel'].isin([1])] #create new bot dataframes
  normal_df = df[df['ActivityLabel'].isin([0])] #create new normal dataframes
  botnet_df.reset_index(drop=True, inplace=True) #reset index from parent dataframe
  normal_df.reset_index(drop=True, inplace=True) #reset index from parent dataframe

  return botnet_df, normal_df

def detectionWithSimilarity():
  ctx = 'DETECTION WITH SIMILARITY'
  detectionResultCollectionName = 'detection-result'
  # choosenDir = menu.getListTestData()
  datasetIndex = menu.getListDatasetMenu()
  datasetName = loader.listAvailableDatasets[int(datasetIndex)]['list']
  datasetDetail = menu.getListDatasetDetailMenu(datasetIndex)
  stringDatasetName = loader.listAvailableDatasets[int(datasetIndex)]['shortName']
  start= watcherStart(ctx)

  selected = 'scenario'+str(datasetDetail)
  print('\tProcessing dataset '+stringDatasetName+' scenario/sensors '+str(datasetDetail)+'...')
  # df = pd.read_csv(choosenDir)
  df = loader.loadDataset(datasetName, selected)

  print(df)
  df['ActivityLabel'] = df['Label'].str.contains('botnet', case=False, regex=True).astype(int)
  df = pp.labelGenerator(df)
  df.sort_values(by='StartTime', inplace=True)
  df.reset_index(drop=True, inplace=True)
  df = pp.transformation(df)

  sources = stringDatasetName+'-'+selected
  sourcesQuery = { 'sources': sources }
  values = saa.sequentialActivityMining(df, stringDatasetName, datasetDetail, sources, detectionResultCollectionName)
  saa.similarityMeasurement(sourcesQuery, detectionResultCollectionName, values )
  # saa.reportDocumentation(sourcesQuery)

  watcherEnd(ctx, start)


def detectionWithSimilarityMulti():
  ctx = 'DETECTION WITH SIMILARITY MULTI PROCESS'
  detectionResultCollectionName = 'detection-result'

  datasetIndex = menu.getListDatasetMenu()
  datasetName = loader.listAvailableDatasets[int(datasetIndex)]['list']
  stringDatasetName = loader.listAvailableDatasets[int(datasetIndex)]['shortName']
  listSubDataset = loader.listAvailableDatasets[int(datasetIndex)]['list']
  start= watcherStart(ctx)
  
  for datasetDetail in range(1, len(listSubDataset)+1):
    selected = 'scenario'+str(datasetDetail)
    print('\tProcessing dataset '+stringDatasetName+' scenario/sensors '+str(datasetDetail)+'...')
    # df = pd.read_csv(choosenDir)
    df = loader.loadDataset(datasetName, selected)

    df['ActivityLabel'] = df['Label'].str.contains('botnet', case=False, regex=True).astype(int)
    df = pp.labelGenerator(df)
    df.sort_values(by='StartTime', inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = pp.transformation(df)

    sources = stringDatasetName+'-'+selected
    sourcesQuery = { 'sources': sources }
    values = saa.sequentialActivityMining(df, stringDatasetName, datasetDetail, sources, detectionResultCollectionName)
    saa.similarityMeasurement(sourcesQuery, detectionResultCollectionName, values)
    # saa.reportDocumentation(sourcesQuery)

  watcherEnd(ctx, start)

def listBotIp(dataset, detail):
  if(dataset== 'ctu' or dataset=='ncc'):
    if(detail==9 or detail==10):
      return ['147.32.84.165', '147.32.84.191', '147.32.84.192', '147.32.84.193',
      '147.32.84.204', '147.32.84.205', '147.32.84.206', '147.32.84.207',
      '147.32.84.208', '147.32.84.209']
    elif(detail==11 or detail==12):
      return ['147.32.84.165', '147.32.84.191', '147.32.84.192']
    else:
      return ['147.32.84.165']
  else:
    if(detail==2):
      return ['147.32.84.193','147.32.84.165','147.32.84.192','147.32.84.209',
      '147.32.84.207','147.32.84.191','147.32.84.205','147.32.84.208',
      '147.32.84.206','147.32.84.204']
    else:
      return ['147.32.84.165','147.32.84.192','147.32.84.193','147.32.84.205',
      '147.32.84.204','147.32.84.207','147.32.84.206','147.32.84.209',
      '222.160.227.154','147.32.84.191','147.32.84.208','95.65.17.47',
      '91.188.37.153','38.229.70.20','93.103.254.175','69.104.66.134','161.200.133.204']

def getReportData(tpfp, dataset, detail, threshold):
  reportCollection = 'report'
  condition = '$in'
  if(tpfp == 'fp'): condition='$nin'
  queryPipeline=[
    { '$match':{
        'FromDatasets': dataset, 'DatasetsDetails':str(detail), 
        'SrcAddr': { condition:listBotIp(dataset,detail) },
        '$or':[
            {'SimilarityScorePer':{ '$gte': threshold}},
            {'SimilarityScoreSim':{ '$gte': threshold}},
            {'SimilarityScoreSpo':{ '$gte': threshold}},
            ]   
    }},
    {
        '$unwind': '$NetworkActivities'
    },
    {
        '$group':{
            '_id': "",
            'avgSimSpo':{ '$avg':'$SimilarityScoreSpo' },
            'avgSimPer':{ '$avg':'$SimilarityScorePer' },
            'avgSimSim':{ '$avg':'$SimilarityScoreSim' },
            'minSimSpo':{ '$min':'$SimilarityScoreSpo' },
            'minSimPer':{ '$min':'$SimilarityScorePer' },
            'minSimSim':{ '$min':'$SimilarityScoreSim' },
            'maxSimSpo':{ '$max':'$SimilarityScoreSpo' },
            'maxSimPer':{ '$max':'$SimilarityScorePer' },
            'maxSimSim':{ '$max':'$SimilarityScoreSim' },
            'count': {'$sum':1}
        }
    }
  ]
  recoredReport = aggregate(queryPipeline, reportCollection)
  recoredReport = recoredReport[0]
  
  # list of column names 
  field_names = 'avgSimSpo,avgSimPer,avgSimSim,minSimSpo,minSimPer,minSimSim,maxSimSpo, maxSimPer, maxSimSim,count,TP/FP,dataset,subDataset,threshold,createdAt'
  del recoredReport['_id']
  recoredReport['TP/FP'] = tpfp
  recoredReport['dataset'] = dataset
  recoredReport['subDataset'] = detail
  recoredReport['threshold'] = threshold
  recoredReport['createdAt'] = datetime.now()

  return list(recoredReport.values())

def reportDocumentation(tpfp='tp', dataset='ctu', detail=7, threshold=0.5):
  ctx='REPORT DOCUMENTATION'
  datasetIndex = menu.getListDatasetMenu()
  datasetName = loader.listAvailableDatasets[int(datasetIndex)]['list']
  stringDatasetName = loader.listAvailableDatasets[int(datasetIndex)]['shortName']
  listSubDataset = loader.listAvailableDatasets[int(datasetIndex)]['list']
  start = watcherStart(ctx)

  recoredReport = []
  for datasetDetail in range(1, len(listSubDataset)+1):
    print('\t..Getting Data from '+stringDatasetName+datasetDetail)
    for threshold in range(0,10):
      recoredReport.append(getReportData('tp', stringDatasetName, datasetDetail, threshold/10))
      recoredReport.append(getReportData('fp', stringDatasetName, datasetDetail, threshold/10))

  print('Start Export the report to csv...')
  with open('data/report_documentation.csv', 'a', newline='', encoding='utf-8') as csv_file:
    writer = csv.writer(csv_file)
    for data in recoredReport:
      writer.writerow(data)

  watcherEnd(ctx, start)
