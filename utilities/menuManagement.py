import time
import os
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
    'title': '[Knowledgebase Management] Export(dump) yours',
    'functionName': mongo.dump
  },
  {
    'title': '[Knowledgebase Management] Restore',
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
    'title': 'EXIT',
    'functionName': exit
  }
]

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
  return choose

def getListDatasetDetailMenu(datasetIndex):
  os.system("clear")
  chooseDataset = dl.listAvailableDatasets[int(datasetIndex)-1]
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
  return choose

def getListTestData():
  os.system("clear")
  print("\n\n------------------------------------------")
  print("\t| List of Test Dataset |")
  print("------------------------------------------\n")
  dir_path = r'data\\testDataset\\'
  for path in os.scandir(dir_path):
      if path.is_file():
          print(path.name)


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
  if(menuIndex < len(listMenu)):
    print("============| Processing Menu: "+str(menuIndex+1)+". "+listMenu[menuIndex]['title']+" |========================\n")
    # execute the function
    listMenu[menuIndex]['functionName']()

    # time.sleep(3) # adding 3 seconds time delay
    # os.system("clear")
    print("\n\t\t"+listMenu[menuIndex]['title']+" Process Success...")
    print("...back to menu...")
  else:
    print("This Menu Does Not Exist, Please Try Another Input!")
    time.sleep(3) # adding 3 seconds time delay
    os.system("clear")
    mainMenu() #call menu again

def mainMenu():
  banner()
  choose = input("Enter Menu: ")
  execute(int(choose)-1)