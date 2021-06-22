import logging

import gensim
import gensim.corpora as corpora
import pandas
from pyLDAvis import prepared_data_to_html
from pyLDAvis.gensim_models import prepare
from loguru import logger

from operators.texera_blocking_unsupervised_trainer_operator import TexeraBlockingUnsupervisedTrainerOperator

# to change library's logger setting
logging.getLogger("gensim").setLevel(logging.ERROR)
logging.getLogger("pyLDAvis").setLevel(logging.ERROR)

class TopicModelingTrainer(TexeraBlockingUnsupervisedTrainerOperator):

    def open(self, *args):
        super(TopicModelingTrainer, self).open(*args)

        # TODO: _train_args from user input args
        if len(args) >= 2:
            self._input_col_name = str(args[0])
            self._train_args = {"num_topics": int(args[1])}
        else:
            raise RuntimeError("Not enough arguments in topic modeling operator.")

        logger.debug(f"getting args {args}")
        logger.debug(f"parsed training args {self._train_args}")

    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        # override accept to accept rows as lists
        self._data.append(row[self._input_col_name].strip().split())

    @staticmethod
    def train(data, *args, **kwargs):
        logger.debug(f"start training, args:{args}, kwargs:{kwargs}")

        # Create Dictionary
        id2word = corpora.Dictionary(data)

        # Create Corpus
        texts = data

        # Term Document Frequency
        corpus = [id2word.doc2bow(text1) for text1 in texts]

        lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus,
                                                    id2word=id2word,
                                                    num_topics=kwargs["num_topics"],
                                                    random_state=100,
                                                    update_every=1,
                                                    chunksize=100,
                                                    passes=10,
                                                    alpha='auto',
                                                    per_word_topics=True)

        pyldaVis_prepared_model = prepare(lda_model, corpus, id2word, n_jobs=1)
        return pyldaVis_prepared_model

    def report(self, model):
        logger.debug(f"reporting trained results")
        html_output = prepared_data_to_html(model)
        self._result_tuples.append(pandas.Series({"output": html_output}))


operator_instance = TopicModelingTrainer()
if __name__ == '__main__':
    """
    The following lines can be put in the file and name it tokenized.txt:

    yes unfortunately use tobacco wrap
    nothing better coming home pre rolled blunt waiting work fact
    sell pre roll blunts
    dutch backwoods hemp wrap
    damn need wrap hemparillo cali fire please

    """

    file1 = open("tokenized.txt", "r+")
    df = file1.readlines()

    operator_instance.open()
    for row in df:
        operator_instance.accept(pandas.Series([row]))
    operator_instance.input_exhausted()
    while operator_instance.has_next():
        print(operator_instance.next())

    file1.close()
    operator_instance.close()
