import logging

# Use gensim 3.8.3
import gensim
import gensim.corpora as corpora
import pandas
import os

from operators.texera_blocking_unsupervised_trainer_operator import TexeraBlockingUnsupervisedTrainerOperator
from operators.texera_udf_operator_base import log_exception

# to change library's logger setting
logging.getLogger("gensim").setLevel(logging.ERROR)

class TopicModeling(TexeraBlockingUnsupervisedTrainerOperator):
    logger = logging.getLogger("PythonUDF.TopicModelingMalletTrainer")

    @log_exception
    def open(self, *args):
        super(TopicModeling, self).open(*args)

        # TODO: _train_args from user input args
        if len(args) >= 3:
            MALLET_HOME = str(args[1])
            NUM_TOPICS = int(args[2])
        else:
            raise RuntimeError("Not enough arguments in topic modeling mallet operator")

        MALLET_PATH = os.path.join(MALLET_HOME,"bin","mallet")
        # We need to fix a seed value so that the output of LDA is deterministic i.e. same output every time.
        # The below value is just an arbitrarily chosen value.
        RANDOM_SEED = 41
        os.environ['MALLET_HOME'] = MALLET_HOME

        self._train_args = {"mallet_path":MALLET_PATH, "random_seed":RANDOM_SEED, "num_topics":NUM_TOPICS}

        self.logger.debug(f"getting args {args}")
        self.logger.debug(f"parsed training args {self._train_args}")

    @log_exception
    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        # override accept to accept rows as lists
        self._data.append(row[0].strip().split())

    @staticmethod
    @log_exception
    def train(data, mallet_path: str, random_seed: int, num_topics: int, *args, **kwargs):
        TopicModeling.logger.debug(f"start training, args:{args}, kwargs:{kwargs}")

        # Create Dictionary
        id2word = corpora.Dictionary(data)

        # Create Corpus
        texts = data

        # Term Document Frequency
        corpus = [id2word.doc2bow(text1) for text1 in texts]

        lda_mallet_model = gensim.models.wrappers.LdaMallet(mallet_path, corpus=corpus, num_topics=num_topics, id2word=id2word, random_seed = random_seed)

        return lda_mallet_model

    @log_exception
    def report(self, model):
        self.logger.debug(f"reporting trained results")
        for id, topic in model.print_topics(num_topics=self._train_args["num_topics"]):
            self._result_tuples.append(pandas.Series({"output": topic}))


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
