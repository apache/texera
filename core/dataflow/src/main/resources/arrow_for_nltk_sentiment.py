import sys
import pickle
import csv

pickleFullPathFileName = sys.argv[1]
dataFullPathFileName = sys.argv[2]
resultFullPathFileName = sys.argv[3]
inputDataMap = {}
recordLabelMap = {}

# call format:
# python3 nltk_sentiment_classify pickleFullPathFileName dataFullPathFileName resultFullPathFileName
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
	pickleFile = open(pickleFullPathFileName, 'rb')
	sentimentModel = pickle.load(pickleFile)#	for text in sys.argv[2:]:
	for key, value in inputDataMap.items():
		recordLabelMap[key] = 1 if sentimentModel.classify(value) == "pos" else -1
	pickleFile.close()
		
def readData():
	with open(dataFullPathFileName, newline='') as csvfile:
		dataReader = csv.reader(csvfile, delimiter=',', quotechar='"')
		for record in dataReader:
			inputDataMap[record[0]] = record[1]
			
if __name__ == "__main__":
	main()
	