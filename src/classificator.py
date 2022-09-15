import csv

from utilities.watcher import *
from numba import jit

from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC
from sklearn.linear_model import *
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score, recall_score, f1_score, accuracy_score

algorithmDict = {
  'decisionTree': DecisionTreeClassifier(),
  'randomForest': RandomForestClassifier(n_estimators=100),
  'svc' : SVC(kernel='linear', C=10.0, random_state=1),
  'naiveBayes': GaussianNB(),
  'knn': KNeighborsClassifier(),
  'logisticRegression' : LogisticRegression(C=10000, solver='liblinear')
}

@jit(nopython=True) # Set "nopython" mode for best performance
def classification(df, train, test, selectedScenario='', algorithm='randomForest'):
  ctx= algorithm.upper()+'-MACHINE LEARNING CLASSIFICATION'
  start= watcherStart(ctx)

  x_df=df.drop(['Label','ActivityLabel'],axis=1)
  y_df=df['ActivityLabel']
  x_train=train.drop(['StartTime','SrcAddr','DstAddr','Dir','sTos','dTos','Label','ActivityLabel','BotnetName','SensorId'],axis=1)
  y_train=train['ActivityLabel']
  x_test=test.drop(['StartTime','SrcAddr','DstAddr','Dir','sTos','dTos','Label','ActivityLabel','BotnetName','SensorId'],axis=1)
  y_test=test['ActivityLabel']

  model = algorithmDict[algorithm]
  model.fit(x_train, y_train)
  predictionResult = model.predict(x_test)
  tn, fp, fn, tp = confusion_matrix(y_df, predictionResult).ravel()
  precision = precision_score(y_test, predictionResult, average='micro')
  recall = recall_score(y_test, predictionResult)
  f1 = f1_score(y_test, predictionResult)

  print('\nTotal input data\t\t\t\t: '+str(x_test.shape[0]))
  print('TN (predict result normal, actual normal)\t: '+str(tn))
  print('FP (predict result bot, actual normal)\t\t: '+str(fp))
  print('FN (predict result normal, actual bot)\t\t: '+str(fn))
  print('TP (predict result bot, actual bot)\t\t: '+str(tp))

  accManual = (tp+tn)/(tp+tn+fp+fn)
  print('Acuracy formulas\t\t\t\t: (TP+tTN)/(TP+TN+FP+FN)')
  print('Accuracy\t\t\t\t\t: '+str(accManual))
  
  # list of column names 
  field_names = ['Scenario','CreatedAt','Algorithm','Accuracy','Precision','Recall', 'F1']
    
  # Dictionary
  dict = {"Scenario": selectedScenario, "CreatedAt": datetime.now(), "Algorithm":algorithm, "Accuracy":accManual, "Precision":precision, "Recall": recall, "F1": f1}

  with open('data/classification_results.csv', 'a') as csv_file:
      dict_object = csv.DictWriter(csv_file, fieldnames=field_names) 
      dict_object.writerow(dict)

  watcherEnd(ctx, start)
  return predictionResult
