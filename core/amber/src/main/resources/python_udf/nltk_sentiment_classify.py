import pickle

import pandas

import texera_udf_operator_base


class NLTKSentimentOperator(texera_udf_operator_base.TexeraMapOperator):

    def __init__(self):
        super(NLTKSentimentOperator, self).__init__(self.predict)
        self._model_file = None
        self._sentiment_model = None

    def open(self, *args):
        super(NLTKSentimentOperator, self).open(*args)
        model_file_path = args[2]
        self._model_file = open(model_file_path, 'rb')
        self._sentiment_model = pickle.load(self._model_file)

    def close(self):
        self._model_file.close()

    def predict(self, row: pandas.Series, *args):
        p = 1 if self._sentiment_model.classify(row[args[0]]) == 'pos' else 0
        row[args[1]] = p
        return row


operator_instance = NLTKSentimentOperator()
