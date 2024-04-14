package edu.uci.ics.texera.workflow.operators.huggingFace

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

class HuggingFaceSentimentAnalysisOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("column to perform sentiment analysis on")
  @AutofillAttributeName
  var attribute: String = _

  override def generatePythonCode(): String = {
    s"""from pytexera import *
       |from transformers import pipeline
       |from transformers import AutoModelForSequenceClassification
       |from transformers import TFAutoModelForSequenceClassification
       |from transformers import AutoTokenizer, AutoConfig
       |import numpy as np
       |from scipy.special import softmax
       |
       |def preprocess(text):
       |    new_text = []
       |    for t in text.split(" "):
       |        t = '@user' if t.startswith('@') and len(t) > 1 else t
       |        t = 'http' if t.startswith('http') else t
       |        new_text.append(t)
       |    return " ".join(new_text)
       |
       |class ProcessTupleOperator(UDFOperatorV2):
       |
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        MODEL = f"cardiffnlp/twitter-roberta-base-sentiment-latest"
       |        tokenizer = AutoTokenizer.from_pretrained(MODEL)
       |        config = AutoConfig.from_pretrained(MODEL)
       |        model = AutoModelForSequenceClassification.from_pretrained(MODEL)
       |        text = preprocess(tuple_["$attribute"])
       |        encoded_input = tokenizer(text, return_tensors='pt')
       |        output = model(**encoded_input)
       |        scores = output[0][0].detach().numpy()
       |        scores = softmax(scores)
       |        ranking = np.argsort(scores)
       |        ranking = ranking[::-1]
       |        for i in range(scores.shape[0]):
       |            l = config.id2label[ranking[i]]
       |            s = scores[ranking[i]]
       |            tuple_[l] = np.round(float(s), 4)
       |        yield tuple_""".stripMargin
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hugging Face Sentiment Analysis",
      "Loading Twitter-Based Sentiment Analysis Model from Hugging Face",
      OperatorGroupConstants.MACHINE_LEARNING_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema
      .builder()
      .add(schemas(0))
      .add("positive", AttributeType.DOUBLE)
      .add("neutral", AttributeType.DOUBLE)
      .add("negative", AttributeType.DOUBLE)
      .build()
  }
}
