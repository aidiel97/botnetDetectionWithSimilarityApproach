import time
import os
import glob

import utilities.mongoDb as mongo
import utilities.dataLoader as dl
import src.knowledgebaseGenerator as kbgen
import src.dataSplitter as ds
import src.detector as detect

listMenu =[
  {
    'title': '[Knowledgebase Management] Generate - Single scenarios/sensors',
    'functionName': kbgen.singleDatasetDetail
  },
  {
    'title': '[Knowledgebase Management] Generate - All Scenarios/sensors',
    'functionName': kbgen.multiDatasetDetail
  },
  {
    'title': '[Knowledgebase Management] Export(Dump) yours',
    'functionName': mongo.dump
  },
  {
    'title': '[Knowledgebase Management] Import(Restore)',
    'functionName': mongo.restore
  },
  {
    'title': '[Data Test Management] Generate',
    'functionName': ds.generateDataTest
  },
  {
    'title': '[Detection] Botnet Detection With Similarity Approach',
    'functionName': detect.detectionWithSimilarity
  },
  {
    'title': '[Multiple Detection] Botnet Detection With Similarity Approach',
    'functionName': detect.detectionWithSimilarityMulti
  },
  {
    'title': 'Generate Report',
    'functionName': detect.reportDocumentation
  },
  {
    'title': 'EXIT',
    'functionName': exit
  }
]

def validateInput(input):
  try:
    input = int(input)-1
    isValid = True
  except ValueError:
    isValid = False
  return isValid, input

def getListDatasetMenu():
  os.system("clear")
  x=1
  print("\n\n------------------------------------------")
  print("\t| List Available Datasets |")
  print("------------------------------------------\n")
  for dataset in dl.listAvailableDatasets:
    print(str(x)+". "+dataset['name'])
    x += 1
  print("\n------------------------------------------")
  choose = input("Choose one dataset: ")
  isValid, choose = validateInput(choose)
  if(isValid == False or choose >= len(dl.listAvailableDatasets)):
    print("This Option Does Not Exist, Bring You Back to Main Menu!")
    time.sleep(3) # adding 3 seconds time delay
    os.system("clear")
    mainMenu() #call menu again
  return choose

def getListDatasetDetailMenu(datasetIndex):
  os.system("clear")
  chooseDataset = dl.listAvailableDatasets[datasetIndex]
  listSubDataset = list(chooseDataset['list'].keys())
  x=1
  print("\n\n------------------------------------------")
  print("\t| List Sub Dataset on "+chooseDataset['name']+" |")
  print("------------------------------------------\n")
  for subDataset in listSubDataset:
    print(str(x)+". "+subDataset)
    x += 1
  print("\n------------------------------------------")
  choose = input("Choose one subDataset: ")
  isValid, choose = validateInput(choose)
  if(isValid == False or choose >= len(listSubDataset)):
    print("This Option Does Not Exist, Bring You Back to Main Menu!")
    time.sleep(3) # adding 3 seconds time delay
    os.system("clear")
    mainMenu() #call menu again
  return choose+1

def getListTestData():
  os.system("clear")
  print("\n\n------------------------------------------")
  print("\t| List of Test Dataset |")
  print("------------------------------------------\n")
  x=1
  dir_path = r'data\testDataset\*.*'
  listFiles = glob.glob(dir_path)
  for subFiles in listFiles:
    print(str(x)+". "+subFiles)
    x += 1
  
  print("\n------------------------------------------")
  choose = input("Choose one subFiles: ")
  isValid, choose = validateInput(choose)
  if(isValid and choose < len(listFiles)):
    choosenDir = listFiles[choose]
  else:
    choosenDir = listFiles[0]
    print("This Option Does Not Exist, Automate Selected to: "+choosenDir)
  return choosenDir


def banner():
  x=1
  print("\n\n================================================================")
  print("======| Botnet Detection with Similarity Approach |=============")
  print("================================================================\n")
  print("Main Menu: ")
  for menu in listMenu:
    print(str(x)+". "+menu['title'])
    x += 1
  print("\n================================================================")

def execute(menuIndex):
  isValid, menuIndex = validateInput(menuIndex)
  if(isValid and menuIndex < len(listMenu)):
    print("============| Processing Menu: "+str(menuIndex+1)+". "+listMenu[menuIndex]['title']+" |========================\n")
    # execute the function
    listMenu[menuIndex]['functionName']()

    # time.sleep(3) # adding 3 seconds time delay
    # os.system("clear")
    print("\n\t\t"+listMenu[menuIndex]['title']+" Process Success...")
    print("\tback to menu...")
  else:
    print("This Menu Does Not Exist, Please Try Another Input!")
    time.sleep(3) # adding 3 seconds time delay
    os.system("clear")
    mainMenu() #call menu again

def mainMenu():
  banner()
  choose = input("Enter Menu: ")
  execute(choose)