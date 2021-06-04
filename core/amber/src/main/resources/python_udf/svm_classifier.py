import pickle

import pandas

from mock_data import df_from_mysql
from operators.texera_map_operator import TexeraMapOperator


class SVMClassifier(TexeraMapOperator):

    def __init__(self):
        super(SVMClassifier, self).__init__(self.predict)
        self._model_file_path = None
        self._vc = None
        self._clf = None

    def open(self, *args):
        super(SVMClassifier, self).open(*args)
        self._model_file_path = args[-1]
        with open(self._model_file_path, 'rb') as file:
            self._vc, self._clf = pickle.load(file)

    def predict(self, row: pandas.Series, *args) -> pandas.Series:
        input_col, output_col, *_ = args
        row[output_col] = self._clf.predict(self._vc.transform([row[input_col]]))[0]
        return row


operator_instance = SVMClassifier()
if __name__ == '__main__':
    df = df_from_mysql("select text from texera_db.test_tweets")
    operator_instance.open("text", "inferred_output", "tobacco_svm.model")
    for index, row in df.iterrows():
        operator_instance.accept(row)
        while operator_instance.has_next():
            print(operator_instance.next())

    operator_instance.close()
