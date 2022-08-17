from utilities.watcher import *

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

def classification(df, train, test, algorithm='randomForest'):
  ctx= algorithm.upper()+'-MACHINE LEARNING CLASSIFICATION'
  start= watcherStart(ctx)

  x_df=df.drop(['Label','activityLabel'],axis=1)
  y_df=df['activityLabel']
  x_train=train.drop(['Label','activityLabel'],axis=1)
  y_train=train['activityLabel']
  x_test=test.drop(['Label','activityLabel'],axis=1)
  y_test=test['activityLabel']

  model = algorithmDict[algorithm]
  model.fit(x_train, y_train)
  predictionResult = model.predict(x_test)
  tn, fp, fn, tp = confusion_matrix(y_df, predictionResult).ravel()

  print('\nTotal input data\t\t\t\t: '+str(x_test.shape[0]))
  print('TN (predict result normal, actual normal)\t: '+str(tn))
  print('FP (predict result bot, actual normal)\t\t: '+str(fp))
  print('FN (predict result normal, actual bot)\t\t: '+str(fn))
  print('TP (predict result bot, actual bot)\t\t: '+str(tp))

  accManual = (tp+tn)/(tp+tn+fp+fn)
  print('Acuracy formulas\t\t\t\t: (TP+tTN)/(TP+TN+FP+FN)')
  print('Accuracy\t\t\t\t\t: '+str(accManual))
  
  watcherEnd(ctx, start)
  return predictionResult
