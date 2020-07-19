import sys
import pickle
import pyarrow as pa
import pandas

pickleFullPathFileName = sys.argv[1]
dataFullPathFileName = sys.argv[2]
resultFullPathFileName = sys.argv[3]
global inputDataFrame, outputDataFrame

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
	with open(resultFullPathFileName, 'wb') as sink:
		table = pa.Table.from_pandas(outputDataFrame)
		writer = pa.ipc.new_file(sink, table.schema)
		writer.write_table(table, max_chunksize=1000)
		writer.close()

def classifyData():
	global inputDataFrame, outputDataFrame
	pickleFile = open(pickleFullPathFileName, 'rb')
	sentimentModel = pickle.load(pickleFile)#	for text in sys.argv[2:]:
	outputDataFrame = pandas.DataFrame(inputDataFrame['ID'])
	preds = []
	for index, row in inputDataFrame.iterrows():
		p = 1 if sentimentModel.classify(row['text']) == "pos" else -1
		preds.append(p)
	outputDataFrame['pred'] = preds
	pickleFile.close()

def readData():
	global inputDataFrame
	inputDataFrame = pa.ipc.open_file(open(dataFullPathFileName, 'rb')).read_pandas()

if __name__ == "__main__":
	main()
