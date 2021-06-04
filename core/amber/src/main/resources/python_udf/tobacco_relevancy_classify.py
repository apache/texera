import pickle

import pandas
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

from operators.texera_map_operator import TexeraMapOperator


def lower_case(text):
    # make all words lower case
    text = text.lower()
    return text


def remove_stopwords(text):
    # remove natural language stop words in the text
    words = [w for w in text if w not in stopwords.words('english')]
    return words


def combine_text(list_of_word):
    return ' '.join(list_of_word)


def text_preprocessing(data, column):
    # print('preprocessing data...')
    data[column] = lower_case(data[column])
    # print('finish lower case...')
    data[column] = word_tokenize(data[column])
    # print('finish tokenize...')
    data[column] = remove_stopwords(data[column])
    # print('finish remove stop words...')
    data[column] = combine_text(data[column])
    # print('finish combine text...')

    return data


class TobaccoClassifier(object):

    def __init__(self, cv_dir='tobacco_cv.sav', model_dir='tobacco_model.sav'):
        # model used to do classification
        self.count_vectorizer = pickle.load(open(cv_dir, 'rb'))
        self.model = pickle.load(open(model_dir, 'rb'))

    def predict(self, test_data, column):
        test_data = text_preprocessing(test_data, column)
        test_vector = self.count_vectorizer.transform([test_data[column]])
        return self.model.predict(test_vector)


class TobaccoRelevancyOperator(TexeraMapOperator):

    def __init__(self):
        super(TobaccoRelevancyOperator, self).__init__(self.predict)
        self._cv_model_path = None
        self._classifier_model_path = None
        self._classifier = None

    def open(self, *args):
        super(TobaccoRelevancyOperator, self).open(*args)
        self._cv_model_path = args[2]
        self._classifier_model_path = args[3]
        self._classifier = TobaccoClassifier(cv_dir=self._cv_model_path, model_dir=self._classifier_model_path)

    def predict(self, row: pandas.Series, *args):
        input_col, output_col, *_ = args
        row[output_col] = self._classifier.predict(row, input_col)[0]


operator_instance = TobaccoRelevancyOperator()
