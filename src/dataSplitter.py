"""
In this file there are two types of split datasets:
1. splitDataFrameWithProportion will divide the data into training with testing according to the proportion that is inputted or by default 80% : 20%.
2. splitTestAllDataframe will set aside the training data according to the specified proportion or 80% by default
"""

from fileinput import filename
import pandas as pd
import numpy as np
from datetime import datetime

import utilities.menuManagement as menu
import utilities.dataLoader as loader
from utilities.watcher import *
from utilities.globalConfig import DEFAULT_MACHINE_LEARNING_TRAIN_PROPORTION, TEST_DATASET_LOCATION

defaultTrainProportion = DEFAULT_MACHINE_LEARNING_TRAIN_PROPORTION
#divide the dataset by class, to set the proportion of datatest
def splitDataFrameWithProportion(dataFrame, trainProportion=defaultTrainProportion):
  ctx='Split Data Frame With Proportion'
  start= watcherStart(ctx)

  normal_df=dataFrame[dataFrame['ActivityLabel'].isin([0])] #create new normal custom dataframe
  bot_df=dataFrame[dataFrame['ActivityLabel'].isin([1])] #create a new data frame for bots

  msk_normal = np.random.rand(len(normal_df)) < trainProportion #get random 20% from normal
  msk_bot = np.random.rand(len(bot_df)) < trainProportion #get random 20% from bot

  #split normal dataset
  normal_dfTrain = normal_df[msk_normal]
  normal_dfTest = normal_df[~msk_normal]

  #split normal dataset
  bot_dfTrain = bot_df[msk_bot]
  bot_dfTest = bot_df[~msk_bot]
  
  #combine dataTest and dataTrain
  train = pd.concat([normal_dfTrain, bot_dfTrain])
  test = pd.concat([normal_dfTest, bot_dfTest])

  watcherEnd(ctx, start)
  return train, test

#only take samples for training, testing with all data
def splitTestAllDataframe(dataFrame, trainProportion=defaultTrainProportion):
  ctx='Split Test All Dataframe'
  start= watcherStart(ctx)

  normal_df=dataFrame[dataFrame['ActivityLabel'].isin([0])] #create new normal custom dataframe
  bot_df=dataFrame[dataFrame['ActivityLabel'].isin([1])] #create a new data frame for bots

  msk_normal = np.random.rand(len(normal_df)) < trainProportion #get random 20% from normal
  msk_bot = np.random.rand(len(bot_df)) < trainProportion #get random 20% from bot

  #split normal dataset
  normal_dfTrain = normal_df[msk_normal]

  #split normal dataset
  bot_dfTrain = bot_df[msk_bot]
  
  #combine dataTest and dataTrain
  train = pd.concat([normal_dfTrain, bot_dfTrain])
  test = dataFrame

  watcherEnd(ctx, start)
  return train, test

def generateDataTest(recordingTime=1):
  ctx='Data Test Generator'
  #get input
  datasetIndex = menu.getListDatasetMenu()
  datasetName = loader.listAvailableDatasets[datasetIndex]['list']
  datasetLabel = loader.listAvailableDatasets[datasetIndex]['shortName']
  datasetDetail = menu.getListDatasetDetailMenu(datasetIndex)
  #get input

  start= watcherStart(ctx)
  selected = 'scenario'+str(datasetDetail)
  dataFrame = loader.loadDataset(datasetName, selected)

  dataFrame.sort_values(by='StartTime', inplace=True)
  dataFrame.reset_index(drop=True, inplace=True)

  #get dataset recorded time
  dfStartAt = pd.to_datetime(dataFrame['StartTime'].iloc[0])
  dfEndAt = pd.to_datetime(dataFrame['StartTime'].iloc[-1])
  #get dataset recorded time

  #try to get data test starting point, loop until starting poin > recordingTime
  loopCount = 10 #limited looping
  startingPoint = dataFrame.sample()
  while((dfEndAt - pd.to_datetime(startingPoint.iloc[0]['StartTime'])).seconds//3600 < recordingTime ):
    startingPoint = dataFrame.sample()
    print(loopCount)
    loopCount-=1
    if(loopCount==0):
      break

  dataFrame['DiffWithSample'] = pd.to_datetime(dataFrame['StartTime']) - pd.to_datetime(startingPoint.iloc[0]['StartTime'])
  dataFrame['DiffWithSample'] = dataFrame['DiffWithSample'].dt.total_seconds() //3600
  dataFrameTest = dataFrame.loc[(dataFrame['DiffWithSample'] >=0) & (dataFrame['DiffWithSample'] < recordingTime) ].drop(['DiffWithSample'],axis=1)
  fileName= str(datetime.now().date())+'_'+datasetLabel+'-'+str(datasetDetail)
  dataFrameTest.to_csv( TEST_DATASET_LOCATION+fileName+'.binetflow', index=False)
  print('\n\t\t .... Success Generate Test Dataframe with '+str(len(dataFrameTest))+' row into: '+fileName+'.binetflow')

  watcherEnd(ctx, start)