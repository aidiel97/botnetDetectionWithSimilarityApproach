"""
In this file there are two types of split datasets:
1. splitDataFrameWithProportion will divide the data into training with testing according to the proportion that is inputted or by default 80% : 20%.
2. splitTestAllDataframe will set aside the training data according to the specified proportion or 80% by default
"""

import pandas as pd
import numpy as np

#divide the dataset by class, to set the proportion of datatest
def splitDataFrameWithProportion(dataFrame, trainProportion=0.8):
  normal_df=dataFrame[dataFrame['activityLabel'].isin([0])] #create new normal custom dataframe
  bot_df=dataFrame[dataFrame['activityLabel'].isin([1])] #create a new data frame for bots

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

  return train, test

#only take samples for training, testing with all data
def splitTestAllDataframe(dataFrame, trainProportion=0.8):
  normal_df=dataFrame[dataFrame['activityLabel'].isin([0])] #create new normal custom dataframe
  bot_df=dataFrame[dataFrame['activityLabel'].isin([1])] #create a new data frame for bots

  msk_normal = np.random.rand(len(normal_df)) < trainProportion #get random 20% from normal
  msk_bot = np.random.rand(len(bot_df)) < trainProportion #get random 20% from bot

  #split normal dataset
  normal_dfTrain = normal_df[msk_normal]

  #split normal dataset
  bot_dfTrain = bot_df[msk_bot]
  
  #combine dataTest and dataTrain
  train = pd.concat([normal_dfTrain, bot_dfTrain])
  test = dataFrame

  return train, test