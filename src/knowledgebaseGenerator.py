import src.sequentialActivityAnalysis as saa
import src.preProcessing as pp
import src.detector as detect
import utilities.menuManagement as menu
import utilities.dataLoader as dl

def singleDatasetDetail(settings={}, withMachineLearning=False):
  if(settings=={}):
    datasetIndex = menu.getListDatasetMenu()
    datasetName = dl.listAvailableDatasets[int(datasetIndex)-1]['list']
    datasetDetail = menu.getListDatasetDetailMenu(datasetIndex)
    stringDatasetName = dl.listAvailableDatasets[int(datasetIndex)-1]['shortName']
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
  listSubDataset = dl.listAvailableDatasets[int(datasetIndex)-1]['list']
  settings = {
    'datasetName': dl.listAvailableDatasets[int(datasetIndex)-1]['name'],
    'stringDatasetName': dl.listAvailableDatasets[int(datasetIndex)-1]['shortName'],
    'datasetDetail': 1
  }
  for index in range(1, len(listSubDataset)+1):
    print(settings)
    settings['datasetDetail']+=1
    # singleDatasetDetail(datasetDetail, datasetName, stringDatasetName, withMachineLearning)