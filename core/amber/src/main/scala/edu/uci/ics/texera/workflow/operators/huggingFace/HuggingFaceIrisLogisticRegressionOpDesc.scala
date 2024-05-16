package edu.uci.ics.texera.workflow.operators.huggingFace

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

class HuggingFaceIrisLogisticRegressionOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "petalLengthCmAttribute", required = true)
  @JsonPropertyDescription("attribute in your dataset corresponding to PetalLengthCm")
  @AutofillAttributeName
  var petalLengthCmAttribute: String = _

  @JsonProperty(value = "petalWidthCmAttribute", required = true)
  @JsonPropertyDescription("attribute in your dataset corresponding to PetalWidthCm")
  @AutofillAttributeName
  var petalWidthCmAttribute: String = _

  @JsonProperty(
    value = "prediction class name",
    required = true,
    defaultValue = "Species_prediction"
  )
  @JsonPropertyDescription("output attribute name for the predicted class of species")
  var predictionClassName: String = _

  @JsonProperty(
    value = "prediction probability name",
    required = true,
    defaultValue = "Species_probability"
  )
  @JsonPropertyDescription(
    "output attribute name for the prediction's probability of being a Iris-setosa"
  )
  var predictionProbabilityName: String = _

  /**
    *  Python code to apply a pre-trained liner regression model on the Iris dataset.
    *  For more info about the model, see https://huggingface.co/sadhaklal/logistic-regression-iris.
    *  @return a String representation of the executable Python source code.
    */
  override def generatePythonCode(): String = {
    s"""from pytexera import *
       |import numpy as np
       |import torch
       |import torch.nn as nn
       |from huggingface_hub import PyTorchModelHubMixin
       |
       |class ProcessTupleOperator(UDFOperatorV2):
       |    def open(self):
       |        class LinearModel(nn.Module, PyTorchModelHubMixin):
       |            def __init__(self):
       |                super().__init__()
       |                self.fc = nn.Linear(2, 1)
       |
       |            def forward(self, x):
       |                out = self.fc(x)
       |                return out
       |
       |        self.model = LinearModel.from_pretrained("sadhaklal/logistic-regression-iris")
       |        self.model.eval()
       |
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        training_features_means = [3.72666667, 1.17619048]
       |        training_features_stds = [1.72528903, 0.73788937]
       |        length = tuple_["$petalLengthCmAttribute"]
       |        width = tuple_["$petalWidthCmAttribute"]
       |        features = np.array([[length, width]])
       |        features = ((features - training_features_means) / training_features_stds)
       |        features = torch.from_numpy(features).float()
       |        with torch.no_grad():
       |            logits = self.model(features)
       |        proba = torch.sigmoid(logits.squeeze())
       |        preds = (proba > 0.5).long()
       |        tuple_["$predictionProbabilityName"] = float(proba)
       |        tuple_["$predictionClassName"] = "Iris-setosa" if preds == 1 else "Not Iris-setosa"
       |        yield tuple_""".stripMargin
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hugging Face Iris Logistic Regression",
      "Predict whether an iris is an Iris-setosa using a pre-trained logistic regression model",
      OperatorGroupConstants.HUGGINGFACE_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    if (
      predictionClassName == null || predictionClassName.trim.isEmpty ||
      predictionProbabilityName == null || predictionProbabilityName.trim.isEmpty
    )
      throw new RuntimeException("Result attribute name should not be empty")
    Schema
      .builder()
      .add(schemas(0))
      .add(predictionClassName, AttributeType.STRING)
      .add(predictionProbabilityName, AttributeType.DOUBLE)
      .build()
  }
}
