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
  choosenDir = menu.getListTestData()
  start= watcherStart(ctx)

  df = pd.read_csv(choosenDir)
  df['ActivityLabel'] = df['Label'].str.contains('botnet', case=False, regex=True).astype(int)
  df = pp.labelGenerator(df)
  df.sort_values(by='StartTime', inplace=True)
  df.reset_index(drop=True, inplace=True)

  fileName = choosenDir[len(TEST_DATASET_LOCATION):-len('.binetflow')]
  stringDatasetName = fileName[11:-2]
  datasetDetail = fileName.split('-')[-1]

  saa.sequentialActivityMining(df, stringDatasetName, datasetDetail, fileName, detectionResultCollectionName)
  saa.dimentionalReductionMultiProcess({ 'sources': fileName }, detectionResultCollectionName)
  saa.similarityMeasurement({ 'sources': fileName }, detectionResultCollectionName)

  watcherEnd(ctx, start)