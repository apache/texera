import os
import six
import six.moves.cPickle
import pandas as pd
from tensorflow.keras.models import load_model
from tensorflow.keras.models import Model
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.preprocessing.text import Tokenizer
import texera_udf_operator_base

"""
Requiements:
six                                1.15.0 
tensorflow                         2.4.1
"""

Model_Path = '%s/src/main/resources/python_udf/models'%(os.getcwd())

class ToxModel():
	def __init__(self,model_dir = Model_Path):
		self.model_dir = model_dir
		self.model = None
		self.tokenizer = None
		self.model_name = 'cnn_wiki_tox'
		self.load_model_from_name(self.model_name)

	def load_model_from_name(self, model_name):
		#load the model and tokenizer
		self.model = load_model(os.path.join(self.model_dir, '%s_model.h5' % model_name))
		self.tokenizer = six.moves.cPickle.load(open(os.path.join(self.model_dir, '%s_tokenizer.pkl' % model_name),'rb'), encoding="utf-8")
	
	def prep_text(self, texts):
		#transfer the text to sequences
		self.tokenizer.oov_token = None
		text_sequences = self.tokenizer.texts_to_sequences(texts)
		return pad_sequences(text_sequences, maxlen=250)

	def tox_predict(self, texts):
		#predict the result
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