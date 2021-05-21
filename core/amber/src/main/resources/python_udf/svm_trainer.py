import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import GridSearchCV
from sklearn.svm import SVC

from mock_data import df_from_mysql
from operators.texera_blocking_supervised_trainer_operator import TexeraBlockingSupervisedTrainerOperator


class SVMTrainer(TexeraBlockingSupervisedTrainerOperator):

    def open(self, *args):
        super(SVMTrainer, self).open(*args)
        self._test_ratio = float(args[2])

        # TODO: _train_args from user input args
        self._train_args = {}
        self._model_file_path = args[-1]

    @staticmethod
    def train(x_train, y_train, *args, **kwargs):
        vectorizer = CountVectorizer()

        x_train = vectorizer.fit_transform(x_train)

        # TODO: find a way to overwrite by user input args
        # clf = SVC(**train_args)
        # clf.fit(x_train, y_train)

        tuned_parameters = [{'kernel': ['rbf'], 'gamma': [1e-3, 1e-4],
                             'C': [1, 10, 100, 1000]},
                            {'kernel': ['linear'], 'C': [1, 10, 100, 1000]}]

        clf = GridSearchCV(
            SVC(), tuned_parameters, scoring='%s_macro' % "precision"
        )
        clf.fit(x_train, y_train)
        return vectorizer, clf

    @staticmethod
    def test(model, x_test, y_test, *args, **kwargs):
        vc, clf = model
        return clf.predict(vc.transform(x_test))


operator_instance = SVMTrainer()
if __name__ == '__main__':
    df = df_from_mysql("select text from texera_db.test_tweets")
    df['label'] = np.random.randint(-1, 2, df.shape[0])
    print(df)

    operator_instance.open(None, None, ".33", "svm.model")
    for index, row in df.iterrows():
        operator_instance.accept(row)
    while operator_instance.has_next():
        print(operator_instance.next())

    operator_instance.close()
