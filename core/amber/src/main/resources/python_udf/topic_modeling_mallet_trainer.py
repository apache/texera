import logging

# Use gensim 3.8.3
import gensim
import gensim.corpora as corpora
import pandas
import os

import texera_udf_operator_base


class TopicModeling(texera_udf_operator_base.TexeraBlockingUnsupervisedTrainerOperator):
    logger = logging.getLogger("PythonUDF.TopicModelingTrainer")

    @texera_udf_operator_base.exception(logger)
    def open(self, *args):
        super(TopicModeling, self).open(*args)

        # TODO: _train_args from user input args
        if len(args) >= 3:
            MALLET_HOME = str(args[1])
            NUM_TOPICS = int(args[2])
        else:
            raise RuntimeError("Not enough arguments in topic modeling operator")

        MALLET_PATH = os.path.join(MALLET_HOME,"bin","mallet")
        # We need to fix a seed value so that the output of LDA is deterministic i.e. same output every time.
        # The below value is just an arbitrarily chosen value.
        RANDOM_SEED = 41
        os.environ['MALLET_HOME'] = MALLET_HOME

        self._train_args = {"mallet_path":MALLET_PATH, "random_seed":RANDOM_SEED, "num_topics":NUM_TOPICS}

        self.logger.debug(f"getting args {args}")
        self.logger.debug(f"parsed training args {self._train_args}")

    @texera_udf_operator_base.exception(logger)
    def accept(self, row: pandas.Series, nth_child: int = 0) -> None:
        # override accept to accept rows as lists
        self._data.append(row[0].strip().split())

    @staticmethod
    @texera_udf_operator_base.exception(logger)
    def train(data, *args, **kwargs):
        TopicModeling.logger.debug(f"start training, args:{args}, kwargs:{kwargs}")

        # Create Dictionary
        id2word = corpora.Dictionary(data)

        # Create Corpus
        texts = data

        # Term Document Frequency
        corpus = [id2word.doc2bow(text1) for text1 in texts]

        lda_mallet_model = gensim.models.wrappers.LdaMallet(kwargs["mallet_path"], corpus=corpus, num_topics=kwargs["num_topics"], id2word=id2word, random_seed = kwargs["random_seed"])

        return lda_mallet_model

    @texera_udf_operator_base.exception(logger)
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
