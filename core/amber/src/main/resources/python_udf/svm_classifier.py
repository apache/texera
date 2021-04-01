import shelve

import pandas

import texera_udf_operator_base
from mock_data import df_from_mysql


class SVMClassifier(texera_udf_operator_base.TexeraMapOperator):

    def __init__(self):
        super(SVMClassifier, self).__init__(self.predict)
        self._model_file_path = None
        self._model = None
        self._vc = None

    def open(self, *args):
        super(SVMClassifier, self).open(*args)
        self._model_file_path = args[-1]

    def predict(self, row: pandas.Series, *args):
        if not self._model and not self._vc:
            with shelve.open(self._model_file_path) as db:
                self._model = db['model']
                self._vc = db['vc']
        row[args[1]] = self._model.predict(self._vc.transform([row[args[0]]]))[0]
        return row


operator_instance = SVMClassifier()
if __name__ == '__main__':
    df = df_from_mysql("select text from texera_db.test_tweets")
    print(df)

    operator_instance.open("text", "inferred_output", "tobacco_svm.model")
    for index, row in df.iterrows():
        operator_instance.accept(row)
        while operator_instance.has_next():
            print(operator_instance.next())

    operator_instance.close()
