import pandas as pd
from numba import jit

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

@jit(nopython=True) # Set "nopython" mode for best performance
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

@jit(nopython=True) # Set "nopython" mode for best performance
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

  sourcesQuery = { 'sources': selected }
  values = saa.sequentialActivityMining(df, stringDatasetName, datasetDetail, selected, detectionResultCollectionName)
  vectors = saa.dimentionalReductionMultiProcess(values, detectionResultCollectionName)
  saa.similarityMeasurement(sourcesQuery, detectionResultCollectionName, vectors)
  # saa.reportDocumentation(sourcesQuery)

  watcherEnd(ctx, start)