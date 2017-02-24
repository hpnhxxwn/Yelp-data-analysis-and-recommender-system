import json
import pandas as pd
from sklearn import cross_validation
import sys
import time
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from numpy import mean
#from yelp_utils import numLines, loadData
#from yelp_load import numLines, loadData
from sklearn import tree
from sklearn.model_selection import train_test_split

def classify(filename, technique, percentData, posneg):
    df = pd.read_json(filename, lines=True)
    train_percentage = float(percentData) / 100.0
    

    if technique == 'nb':
        clf_obj = MultinomialNB()
    elif technique == 'svm':
        clf_obj = SGDClassifier(loss='hinge', penalty='l2', alpha=1e-3, n_iter=5, random_state=42)
    elif technique == 'lr':
        clf_obj = LogisticRegression()
    elif technique == 'dt':
        clf_obj = tree.DecisionTreeClassifier(min_samples_split=40)


    start_time = time.time();
    text_clf = Pipeline([("vector", CountVectorizer(stop_words='english', min_df=1)),
    					  ("tfidf", TfidfTransformer()),
    					  ("clf", clf_obj)])

    if (posneg == True):
        print (df["positive label"])
        print (df["text"])
        train, test = train_test_split(df, train_size=train_percentage)
    	text_clf.fit(train["text"], train["positive label"])
        test_label = test["positive label"]
    else:
        train, test = train_test_split(df, train_size=train_percentage)
    	text_clf.fit(train["text"], train["negative label"])
        test_label = test["negative label"]

    predicted = text_clf.predict(test["text"])

    print "time: %s seconds" % (time.time() - start_time)

    print "accuracy:", mean(predicted == test_label)

def print_usage():
    print "Usage: classify.py <filepath> <nb/svm/lr/dt> <True/False> <number in [0, 100]>"
    print "e.g., classify.py nb True 85"

def valid_args(avail_techniques, technique, posneg, percentData):
    return technique in avail_techniques and\
            (posneg == 'True' or posneg == 'False') and\
            (int(percentData) >= 0 and int(percentData) <= 100)

if __name__ == '__main__':
    techniques = {'nb': 'Naive Bayes', 'svm': 'Support Vector Machines', 'lr': 'Logistic Regression', 'dt':'Decision Tree'}

    try:
    	filename = sys.argv[1]
	print (filename)
	print(sys.argv[1])
    	technique = sys.argv[2].lower()
	print(technique)
    	posneg = sys.argv[3] # True or False
	print(posneg)
    	percentData = sys.argv[4] # [0, 100]
	print(percentData)
    except IndexError:
    	print_usage()
        sys.exit(1)

    if not valid_args(techniques.keys(), technique, posneg, percentData):
    	print_usage()
        sys.exit(1)

    if posneg == 'True': posneg = True
    else: posneg = False

    print "Technique:", techniques[technique]
    if posneg: print "Positive/negative classification"
    else: print "5-star classification"
    print "% of data used:", percentData
    classify(filename, technique, percentData, posneg)



