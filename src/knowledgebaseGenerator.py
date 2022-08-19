import src.sequentialActivityAnalysis as saa
import src.preProcessing as pp
import src.detector as detect
import utilities.menuManagement as menu
import utilities.dataLoader as loader

def singleDatasetDetail(settings={}, withMachineLearning=False):
  if(settings=={}):
    datasetIndex = menu.getListDatasetMenu()
    datasetName = loader.listAvailableDatasets[int(datasetIndex)-1]['list']
    datasetDetail = menu.getListDatasetDetailMenu(datasetIndex)
    stringDatasetName = loader.listAvailableDatasets[int(datasetIndex)-1]['shortName']
  else:
    datasetName = settings['datasetName']
    datasetDetail = settings['datasetDetail']
    stringDatasetName = settings['stringDatasetName']

  selected = 'scenario'+str(datasetDetail)
  if(withMachineLearning==True):
    botnet_df, normal_df = detect.detectionWithMachineLearning(datasetName,  selected)
  else:
    botnet_df, normal_df = detect.detectionWithLabel(datasetName, selected)

  botnet_df = pp.labelGenerator(botnet_df)
  saa.sequentialActivityMining(botnet_df, stringDatasetName, datasetDetail)
  saa.sequentialActivityReduction(stringDatasetName, datasetDetail)

def multiDatasetDetail():
  datasetIndex = menu.getListDatasetMenu()
  listSubDataset = loader.listAvailableDatasets[int(datasetIndex)-1]['list']
  settings = {
    'datasetName': loader.listAvailableDatasets[int(datasetIndex)-1]['list'],
    'stringDatasetName': loader.listAvailableDatasets[int(datasetIndex)-1]['shortName'],
  }
  for index in range(1, len(listSubDataset)+1):
    settings['datasetDetail'] = index
    singleDatasetDetail(settings)