import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.svm import SVC

import texera_udf_operator_base
from mock_data import df_from_mysql


class SVMTrainer(texera_udf_operator_base.TexeraBlockingTrainerOperator):

    def open(self, *args):
        super(SVMTrainer, self).open(*args)
        self._train_size = int(args[2])
        self._test_size = int(args[3])
        self._train_args = {'kernel': args[4], 'degree': int(args[5])}
        self.model_filename = args[6]
        self.vc_filename = args[7]

    @staticmethod
    def train(X_train, Y_train, **train_args):
        vectorizer = CountVectorizer()

        X_train = vectorizer.fit_transform(X_train)
        Y_train = Y_train

        svclassifier = SVC(**train_args)
        svclassifier.fit(X_train, Y_train)
        return vectorizer, svclassifier


operator_instance = SVMTrainer()

if __name__ == '__main__':
    df = df_from_mysql("select text from texera_db.test_tweets")
    df['label'] = np.random.randint(-1, 2, df.shape[0])
    print(df)
    operator_instance.open(None, None, "300", "50", "linear", "20", "model.pickle", "vc.pickle")
    for index, row in df.iterrows():
        operator_instance.accept(row)
        print(operator_instance._status)
    while operator_instance.has_next():
        print(operator_instance.next())

    operator_instance.close()
