import csv
import re
import sys
import string
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import nltk

stop_words = stopwords.words('english')
#add punctuation char's to stopwords list
stop_words += list(string.punctuation) # <-- contains !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
#add integers
stop_words += ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

lemmatizer = nltk.stem.WordNetLemmatizer()

def remove_urls(text):
    return re.sub(r"http\S+", "", text)

def tokenize_lowercase(text):
    tokens = nltk.word_tokenize(text)
    stopwords_removed = [token.lower() for token in tokens if token.lower() not in stop_words]
    return ' '.join(stopwords_removed)
 
def remove_nums_and_small_text(text):
    word_list = text.split(" ")
    no_nums = [word for word in word_list if word.isalpha() and len(word)>=3]
    return ' '.join(no_nums)
 
def lemmatize_text(text):
    lemmatized =[]
    for w in text.split(" "):
        lemmatized.append(lemmatizer.lemmatize(w))
    return ' '.join(lemmatized)

def map_function(row, *args):
    input_col, output_col, *_ = args
    url_removed = remove_urls(row[input_col])
    tokenized = tokenize_lowercase(url_removed)
    small_words_removed = remove_nums_and_small_text(tokenized)
    lemmatized = lemmatize_text(small_words_removed)
    row[output_col] = lemmatized
    return row
