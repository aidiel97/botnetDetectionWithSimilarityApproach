"""Botnet Detection with Similarity Approach"""
"""Writen By: M. Aidiel Rachman Putra"""
"""Organization: Net-Centic Computing Laboratory | Institut Teknologi Sepuluh Nopember"""

import warnings
warnings.simplefilter(action='ignore')

import utilities.menuManagement as menu

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