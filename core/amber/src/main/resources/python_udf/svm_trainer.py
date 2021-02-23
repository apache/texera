import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.svm import SVC

import texera_udf_operator_base
from mock_data import df_from_mysql


class SVMTrainer(texera_udf_operator_base.TexeraBlockingTrainerOperator):

    def open(self, *args):
        super(SVMTrainer, self).open(self.training, *args)
        self.train_size = int(args[2])
        self.test_size = int(args[3])
        self.kernel = args[4]
        self.degree = int(args[5])
        self.model_filename = args[6]
        self.vc_filename = args[7]
        self.report_filename = args[8]

    @staticmethod
    def training(X_train, Y_train, kernel, degree):
        vectorizer = CountVectorizer()

        X_train = vectorizer.fit_transform(X_train)
        Y_train = Y_train

        svclassifier = SVC(kernel=kernel, degree=degree)
        svclassifier.fit(X_train, Y_train)
        return vectorizer, svclassifier


operator_instance = SVMTrainer(SVMTrainer.training)

if __name__ == '__main__':
    df = df_from_mysql("select text from texera_db.test_tweets")
    df['label'] = np.random.randint(-1, 2, df.shape[0])
    print(df)
    operator_instance.open(None, None, "300", "50", "model.pickle", "vc.pickle")
    for index, row in df.iterrows():
        operator_instance.accept(row)
        print(operator_instance.status)
    while operator_instance.has_next():
        print(operator_instance.next())

    operator_instance.close()
