import pandas as pd

from utilities.watcher import *
from utilities.globalConfig import DATASET_LOCATION, CTU_DIR, NCC_DIR, NCC2_DIR

#all CTU-13 dataset scenarios
ctuOnline = {
  'scenario1': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-42/detailed-bidirectional-flow-labels/capture20110810.binetflow',
  'scenario2': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-43/detailed-bidirectional-flow-labels/capture20110811.binetflow',
  'scenario3': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-44/detailed-bidirectional-flow-labels/capture20110812.binetflow',
  'scenario4': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-45/detailed-bidirectional-flow-labels/capture20110815.binetflow',
  'scenario5': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-46/detailed-bidirectional-flow-labels/capture20110815-2.binetflow',
  'scenario6': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-47/detailed-bidirectional-flow-labels/capture20110816.binetflow',
  'scenario7': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-48/detailed-bidirectional-flow-labels/capture20110816-2.binetflow',
  'scenario8': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-49/detailed-bidirectional-flow-labels/capture20110816-3.binetflow',
  'scenario9': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-50/detailed-bidirectional-flow-labels/capture20110817.binetflow',
  'scenario10': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-51/detailed-bidirectional-flow-labels/capture20110818.binetflow',
  'scenario11': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-52/detailed-bidirectional-flow-labels/capture20110818-2.binetflow',
  'scenario12': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-53/detailed-bidirectional-flow-labels/capture20110819.binetflow',
  'scenario13': 'https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-54/detailed-bidirectional-flow-labels/capture20110815-3.binetflow',
}

datasetLocation = DATASET_LOCATION
ctuLoc = CTU_DIR
ctu = {
  'scenario1': datasetLocation+ctuLoc+'/1/capture20110810.binetflow',
  'scenario2': datasetLocation+ctuLoc+'/2/capture20110811.binetflow',
  'scenario3': datasetLocation+ctuLoc+'/3/capture20110812.binetflow',
  'scenario4': datasetLocation+ctuLoc+'/4/capture20110815.binetflow',
  'scenario5': datasetLocation+ctuLoc+'/5/capture20110815-2.binetflow',
  'scenario6': datasetLocation+ctuLoc+'/6/capture20110816.binetflow',
  'scenario7': datasetLocation+ctuLoc+'/7/capture20110816-2.binetflow',
  'scenario8': datasetLocation+ctuLoc+'/8/capture20110816-3.binetflow',
  'scenario9': datasetLocation+ctuLoc+'/9/capture20110817.binetflow',
  'scenario10': datasetLocation+ctuLoc+'/10/capture20110818.binetflow',
  'scenario11': datasetLocation+ctuLoc+'/11/capture20110818-2.binetflow',
  'scenario12': datasetLocation+ctuLoc+'/12/capture20110819.binetflow',
  'scenario13': datasetLocation+ctuLoc+'/13/capture20110815-3.binetflow',
}

nccLoc = NCC_DIR
ncc = {
  'scenario1': datasetLocation+nccLoc+'/scenario_dataset_1/dataset_result.binetflow',
  'scenario2': datasetLocation+nccLoc+'/scenario_dataset_2/dataset_result.binetflow',
  'scenario3': datasetLocation+nccLoc+'/scenario_dataset_3/dataset_result.binetflow',
  'scenario4': datasetLocation+nccLoc+'/scenario_dataset_4/dataset_result.binetflow',
  'scenario5': datasetLocation+nccLoc+'/scenario_dataset_5/dataset_result.binetflow',
  'scenario6': datasetLocation+nccLoc+'/scenario_dataset_6/dataset_result.binetflow',
  'scenario7': datasetLocation+nccLoc+'/scenario_dataset_7/dataset_result.binetflow',
  'scenario8': datasetLocation+nccLoc+'/scenario_dataset_8/dataset_result.binetflow',
  'scenario9': datasetLocation+nccLoc+'/scenario_dataset_9/dataset_result.binetflow',
  'scenario10': datasetLocation+nccLoc+'/scenario_dataset_10/dataset_result.binetflow',
  'scenario11': datasetLocation+nccLoc+'/scenario_dataset_11/dataset_result.binetflow',
  'scenario12': datasetLocation+nccLoc+'/scenario_dataset_12/dataset_result.binetflow',
  'scenario13': datasetLocation+nccLoc+'/scenario_dataset_13/dataset_result.binetflow',
}

ncc2Loc = NCC2_DIR
ncc2 = {
  'scenario1': datasetLocation+ncc2Loc+'/all-sensors/sensors-all.binetflow',
  'scenario1': datasetLocation+ncc2Loc+'/sensor1/sensor1.binetflow',
  'scenario2': datasetLocation+ncc2Loc+'/sensor2/sensor2.binetflow',
  'scenario3': datasetLocation+ncc2Loc+'/sensor3/sensor3.binetflow',
}

def loadDataset(dataset, scenario):
  ctx='DATASET LOADER'
  start = watcherStart(ctx)

  fileName = dataset[scenario] #load dataset
  raw_df=pd.read_csv(fileName)

  watcherEnd(ctx, start)
  return raw_df
