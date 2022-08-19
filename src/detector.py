import pandas as pd

import src.classificator as cl
import src.dataSplitter as ds
import src.preProcessing as pp
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
  menu.getListTestData()