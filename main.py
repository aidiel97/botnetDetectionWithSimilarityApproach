"""Botnet Detection with Similarity Approach"""
"""Writen By: M. Aidiel Rachman Putra"""
"""Organization: Net-Centic Computing Laboratory | Institut Teknologi Sepuluh Nopember"""

import warnings
# from botnetDetectionWithSimilarityApproach.src.preProcessing import preProcessing
warnings.simplefilter(action='ignore')

import src.detector as detector
import src.classificator as cl
import src.dataSplitter as ds
import src.preProcessing as pp
import src.sequentialActivityAnalysis as saa
import utilities.dataLoader as dl
import utilities.mongoDb as mongo
import utilities.menuManagement as menu

def detectionWithMachineLearning(datasetName, selectedScenario):
  raw_df = dl.loadDataset(datasetName, selectedScenario)
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
  df = dl.loadDataset(datasetName, selectedScenario)
  df['ActivityLabel'] = df['Label'].str.contains('botnet', case=False, regex=True).astype(int)
  botnet_df = df[df['ActivityLabel'].isin([1])] #create new bot dataframes
  normal_df = df[df['ActivityLabel'].isin([0])] #create new normal dataframes
  botnet_df.reset_index(drop=True, inplace=True) #reset index from parent dataframe
  normal_df.reset_index(drop=True, inplace=True) #reset index from parent dataframe

  return botnet_df, normal_df

if __name__ == "__main__":
  menu.mainMenu()
  # datasetName = ctu
  # stringDatasetName = 'ctu'
  # for datasetDetail in range(9,14):
  #   selectedScenario = 'scenario'+str(datasetDetail)

  #   # botnet_df, normal_df = detectionWithMachineLearning(datasetName,  selectedScenario)
  #   botnet_df, normal_df = detectionWithLabel(datasetName, selectedScenario)
  #   botnet_df = labelGenerator(botnet_df)
  #   sequentialActivityMining(botnet_df, stringDatasetName, datasetDetail)
  #   sequentialActivityReduction(stringDatasetName, datasetDetail)

  # detector.recons()

  # datasetDetail=7
  # datasetName = dl.ctu
  # stringDatasetName = 'ctu'
  # selectedScenario = 'scenario'+str(datasetDetail)
  # # botnet_df, normal_df = detectionWithMachineLearning(datasetName,  selectedScenario)
  # botnet_df, normal_df = detectionWithLabel(datasetName, selectedScenario)
  # botnet_df = pp.labelGenerator(botnet_df)
  # saa.sequentialActivityMining(botnet_df, stringDatasetName, datasetDetail)
  # saa.sequentialActivityReduction(stringDatasetName, datasetDetail)