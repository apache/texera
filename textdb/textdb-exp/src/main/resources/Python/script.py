import sys
from nltk.tokenize import sent_tokenize, word_tokenize
import nltk

def main():
	for line in sys.argv[1:]:
		tags = nltk.pos_tag(word_tokenize(line))
		print(str(tags).strip('[]'))

main()
