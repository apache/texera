import sys
import pickle
import csv
from nltk.tokenize import TweetTokenizer

modelFullPathFileName = sys.argv[1]
dataFullPathFileName = sys.argv[2]
resultFullPathFileName = sys.argv[3]
inputDataMap = {}
recordLabelMap = {}

# call format:
# python3 ml_sentiment_classify dataFullPathFileName resultFullPathFileName
def debugLine(strLine):
	f = open("python_classifier_loader.log","a+")
	f.write(strLine)
	f.close()

def main():
	readData()
	classifyData()
	writeResults()

def writeResults():
	with open(resultFullPathFileName, 'w', newline='') as csvfile:
		resultWriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		resultWriter.writerow(["TupleID", "ClassLabel"])
		for id, classLabel in recordLabelMap.items():
			resultWriter.writerow([id, classLabel])

def classifyData():
	CLASSIFIER = pickle.load(open(modelFullPathFileName, 'rb'))
	tknzr = TweetTokenizer()
	for key, value in inputDataMap.items():
		print(value)
		print(CLASSIFIER.classify({x: True for x in tknzr.tokenize(value)}))
		recordLabelMap[key] = 1 if CLASSIFIER.classify({x: True for x in tknzr.tokenize(value)}) == "POS" else -1

def readData():
	with open(dataFullPathFileName, newline='') as csvfile:
		dataReader = csv.reader(csvfile, delimiter=',', quotechar='"')
		for record in dataReader:
			inputDataMap[record[0]] = record[1]
			
if __name__ == "__main__":
	main()
	
