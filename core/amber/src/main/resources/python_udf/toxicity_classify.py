import os
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import six
import six.moves.cPickle
from numpy import ndarray
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing.sequence import pad_sequences

from operators.texera_map_operator import TexeraMapOperator

"""
Requirements:
six                                1.15.0 
tensorflow                         2.5.0
"""


class ToxModel:
    # to simplify the operator, the model path is hard coded.
    MODEL_PATH = Path(os.getcwd()).joinpath('src', 'main', 'resources', 'python_udf', 'models')
    MODEL_NAME = 'cnn_wiki_tox'

    def __init__(self):
        self._model = load_model(ToxModel.MODEL_PATH.joinpath(f'{ToxModel.MODEL_NAME}_model.h5'))
        with open(ToxModel.MODEL_PATH.joinpath(f'{ToxModel.MODEL_NAME}_tokenizer.pkl'), 'rb') as file:
            self._tokenizer = six.moves.cPickle.load(file, encoding="utf-8")

    def _prep_text(self, texts: Iterable[str]) -> ndarray:
        """
        transfer the given input texts to sequences of integers, padded with a certain length.
        :param texts: an iterable of str to be processed.
        :return: a ndarray of integer sequences.
        """
        return pad_sequences(self._tokenizer.texts_to_sequences(texts), maxlen=250)

    def tox_predict(self, texts: Iterable[str]) -> ndarray:
        """
        predict the toxicity of the given input texts.
        :param texts: an iterable of str to be processed.
        :return: a ndarray of toxicities of each text, from 0.0 ~ 1.0, the exact semantic is to be defined by the ToxModel.
        """
        return self._model.predict(self._prep_text(texts))[:, 1]


class ToxicityClassifier(TexeraMapOperator):

    def __init__(self):
        super(ToxicityClassifier, self).__init__(self.predict)
        self._model = None

    def open(self, *args) -> None:
        super(ToxicityClassifier, self).open(*args)
        self._model = ToxModel()

    def predict(self, row: pd.Series, *args) -> pd.Series:
        input_col, output_col, *_ = args

        # obtain toxicities of all the input data, split by line.
        toxicities = self._model.tox_predict(row[input_col].split('\n'))

        # take an average of toxicities, use it as the toxicity of the input data.
        # using int for now since python udf does not support boolean yet.
        toxic = 1 if np.average(toxicities) > 0.5 else 0

        # assign the result toxic to output column.
        row[output_col] = toxic

        return row


operator_instance = ToxicityClassifier()
