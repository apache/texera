import pickle

import pandas

import texera_udf_operator_base
from mock_data import df_from_mysql


class SVMClassifier(texera_udf_operator_base.TexeraMapOperator):

    def __init__(self):
        super(SVMClassifier, self).__init__(self.predict)
        self._model_file = None
        self._sentiment_model = None
        self._model_file_path = None
        self._vc_file_path = None
        self._model = None
        self._vc = None

    def open(self, *args):
        super(SVMClassifier, self).open(*args)
        self._model_file_path = args[2]
        self._vc_file_path = args[3]

    def predict(self, row: pandas.Series, *args):
        if not self._model:
            with open(self._model_file_path, 'rb') as file:
                self._model = pickle.load(file)
        if not self._vc:
            with open(self._vc_file_path, 'rb') as file:
                self._vc = pickle.load(file)
        row[args[1]] = self._model.predict(self._vc.transform([row[args[0]]]))[0]
        return row


operator_instance = SVMClassifier()
if __name__ == '__main__':
    df = df_from_mysql("select text from texera_db.test_tweets")
    print(df)

    operator_instance.open("text", "inferred_output", "tobacco_model.pickle", "tobacco_vc.pickle")
    for index, row in df.iterrows():
        operator_instance.accept(row)
        while operator_instance.has_next():
            print(operator_instance.next())

    operator_instance.close()
