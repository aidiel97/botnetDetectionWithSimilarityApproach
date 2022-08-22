import pandas as pd

import src.classificator as cl
import src.dataSplitter as ds
import src.preProcessing as pp
import src.sequentialActivityAnalysis as saa
import utilities.dataLoader as loader
import utilities.menuManagement as menu

from utilities.globalConfig import *
from utilities.watcher import *

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

  result = cl.classification(df, train, test, 'randomForest')
  raw_df = raw_df.drop(['activityLabel'],axis=1)
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
  choosenDir = menu.getListTestData()
  start= watcherStart(ctx)

  df = pd.read_csv(choosenDir)
  # df['SrcAddrOrigin'] = df['SrcAddr']
  # df['DstAddrOrigin'] = df['DstAddr']
  # df = pp.transformation(df, False)
  # df = pp.setEmptyString(df)
  # df = pp.normalization(df)
  df['ActivityLabel'] = df['Label'].str.contains('botnet', case=False, regex=True).astype(int)
  df = pp.labelGenerator(df)
  df.sort_values(by='StartTime', inplace=True)
  df.reset_index(drop=True, inplace=True)

  fileName = choosenDir[len(TEST_DATASET_LOCATION):-len('.binetflow')]
  stringDatasetName = fileName[11:-2]
  datasetDetail = fileName.split('-')[-1]
  print(df.columns)
  # saa.sequentialActivityMining(df, stringDatasetName, datasetDetail, fileName, 'detection-result')
  saa.sequentialActivityMiningViaMemory(df, stringDatasetName, datasetDetail, fileName, 'detection-result')

  #get dataset recorded time
  # dfStartAt = pd.to_datetime(df['StartTime'].iloc[0])
  # dfEndAt = pd.to_datetime(df['StartTime'].iloc[-1])
  # dfRecordingTime = (dfEndAt-dfStartAt).seconds
  # df['DiffWithStart'] = pd.to_datetime(df['StartTime']) - dfStartAt
  # df['DiffWithStart'] = df['DiffWithStart'].dt.total_seconds()

  # dfSegmentsCount = round(dfRecordingTime/segmentTime)
  # segmentStartAt = 0
  # segmentEndAt = segmentTime
  # while(segmentEndAt <= dfRecordingTime+1):
  #   segment = df.loc[(df['DiffWithStart'] >= segmentStartAt) & (df['DiffWithStart'] < segmentEndAt) ]
  #   print(segment['StartTime'].iloc[0])
  #   sources = segment['SrcAddrOrigin'].unique()
  #   print(sources)
  #   for ip in sources:
  #     dfSeqAct = segment.loc[df['SrcAddrOrigin'] == ip]
  #     dstUnique = segment['DstAddrOrigin'].unique()
  #     if(len(dstUnique) > 1):
  #       print(dstUnique)
  #     print(dfSeqAct)
  #   print(segment['StartTime'].iloc[-1])
  #   segmentStartAt+=slidingTime
  #   segmentEndAt+=slidingTime

  # segment = df.loc[(df['DiffWithStart'] >= 0) & (df['DiffWithStart'] < 120) ]
  # print(segment['StartTime'].iloc[0])
  # sources = segment['SrcAddrOrigin'].unique()
  # print(sources)
  # for ip in sources:
  #   dfSeqAct = segment.loc[df['SrcAddrOrigin'] == ip]
  #   dstUnique = segment['DstAddrOrigin'].unique()
  #   if(len(dstUnique) > 1):

  #     print(dstUnique)
  #   print(dfSeqAct)
  # print(segment['StartTime'].iloc[-1])
    #sudah bisa dibagi setiap segment, selanjutnya setiap segment dikelompokkan berdasarkan ip, baru analisa similarity, perhatikan kolom yang disertakan waktu analisis
  # .drop(['DiffWithStart'],axis=1)

  watcherEnd(ctx, start)