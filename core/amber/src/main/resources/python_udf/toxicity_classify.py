import os
import six
import six.moves.cPickle
import json
import pickle
import pandas as pd
from tensorflow.keras.models import load_model
from tensorflow.keras.models import Model
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.preprocessing.text import Tokenizer
import texera_udf_operator_base

DEFAULT_MODEL_DIR = '/home/eric/texera/core/amber/src/main/resources/python_udf/models'

DEFAULT_HPARAMS = {
    'max_sequence_length': 250,
    'max_num_words': 10000,
    'embedding_dim': 100,
    'embedding_trainable': False,
    'learning_rate': 0.00005,
    'stop_early': True,
    'es_patience': 1,
    'es_min_delta': 0,
    'batch_size': 128,
    'epochs': 20,
    'dropout_rate': 0.3,
    'cnn_filter_sizes': [128, 128, 128],
    'cnn_kernel_sizes': [5, 5, 5],
    'cnn_pooling_sizes': [5, 5, 40],
    'verbose': True
}
 
class ToxModel():
	def __init__(self,model_dir=DEFAULT_MODEL_DIR):
		self.model_dir = model_dir
		self.model = None
		self.tokenizer = None
		self.model_name = 'cnn_wiki_tox_v3'
		self.hparams = DEFAULT_HPARAMS.copy()
		self.load_model_from_name(self.model_name)

	def load_model_from_name(self, model_name):
		self.model = load_model(os.path.join(self.model_dir, '%s_model.h5' % model_name))
		self.tokenizer = six.moves.cPickle.load(open(os.path.join(self.model_dir, '%s_tokenizer.pkl' % model_name),'rb'), encoding="utf-8")
		with open(os.path.join(self.model_dir, '%s_hparams.json' % self.model_name),'r') as f:
			self.hparams = json.load(f)
 
	def prep_text(self, texts):
		"""Turns text into into padded sequences.

		The tokenizer must be initialized before calling this method.

		Args:
		texts: Sequence of text strings.

		Returns:
		A tokenized and padded text sequence as a model input.
		"""
		self.tokenizer.oov_token = None
		text_sequences = self.tokenizer.texts_to_sequences(texts)
		return pad_sequences(text_sequences, maxlen=self.hparams['max_sequence_length'])

	def tox_predict(self, texts):
		data = self.prep_text(texts)
		return self.model.predict(data)[:, 1]


class ToxModelOperator(texera_udf_operator_base.TexeraMapOperator):

	def __init__(self):
		super(ToxModelOperator, self).__init__()
		self.model_operator = None
                

	def open(self, args: list):
		super(ToxModelOperator, self).open(args)
		self.model_operator = ToxModel()
		self._map_function = self.predict

	def close(self):
		self.model_operator.close()

	def predict(self, row: pd.Series, args: list):
		p = 1 if self.model_operator.tox_predict(row[args[0]].split('\n')) > 0.5 else 0
		row[args[1]] = p
		return row   


operator_instance = ToxModelOperator()
