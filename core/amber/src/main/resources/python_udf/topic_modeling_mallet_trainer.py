import logging
import os

# Use gensim 3.8.3
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

class TopicModeling(TexeraBlockingUnsupervisedTrainerOperator):

    def open(self, *args):
        super(TopicModeling, self).open(*args)

        # TODO: _train_args from user input args
        if len(args) >= 3:
            self._input_col_name = str(args[0])
            MALLET_HOME = str(args[1])
            NUM_TOPICS = int(args[2])
        else:
            raise RuntimeError("Not enough arguments in topic modeling mallet operator")

        MALLET_PATH = os.path.join(MALLET_HOME, "bin", "mallet")
        # We need to fix a seed value so that the output of LDA is deterministic i.e. same output every time.
        # The below value is just an arbitrarily chosen value.
        RANDOM_SEED = 41
        os.environ['MALLET_HOME'] = MALLET_HOME

        self._train_args = {"mallet_path": MALLET_PATH, "random_seed": RANDOM_SEED, "num_topics": NUM_TOPICS}

        logger.debug(f"getting args {args}")
        logger.debug(f"parsed training args {self._train_args}")

    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        # override accept to accept rows as lists
        self._data.append(row[self._input_col_name].strip().split())

    @staticmethod
    def train(data, mallet_path: str, random_seed: int, num_topics: int, *args, **kwargs):
        logger.debug(f"start training, args:{args}, kwargs:{kwargs}")

        # Create Dictionary
        id2word = corpora.Dictionary(data)

        # Create Corpus
        texts = data

        # Term Document Frequency
        corpus = [id2word.doc2bow(text1) for text1 in texts]
        lda_mallet_model = gensim.models.wrappers.LdaMallet(mallet_path, corpus=corpus, num_topics=num_topics, id2word=id2word, random_seed = random_seed)
        # mallet models need to first be converted to gensim models
        gensim_model = gensim.models.wrappers.ldamallet.malletmodel2ldamodel(lda_mallet_model)
        pyldaVis_prepared_model = prepare(gensim_model, corpus, id2word, n_jobs=1)
        return pyldaVis_prepared_model

    def report(self, model):
        logger.debug(f"reporting trained results")
        html_output = prepared_data_to_html(model)
        self._result_tuples.append(pandas.Series({"output": html_output}))


operator_instance = TopicModeling()

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
