package edu.uci.ics.texera.workflow.operators.huggingFace

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

class HuggingFaceIrisLogisticRegressionOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "petalLengthCmAttribute", required = true)
  @JsonPropertyDescription("column in your dataset corresponding to PetalLengthCm")
  @AutofillAttributeName
  var petalLengthCmAttribute: String = _

  @JsonProperty(value = "petalWidthCmAttribute", required = true)
  @JsonPropertyDescription("column in your dataset corresponding to PetalWidthCm")
  @AutofillAttributeName
  var petalWidthCmAttribute: String = _

  @JsonProperty(
    value = "prediction class name",
    required = true,
    defaultValue = "Species_prediction"
  )
  @JsonPropertyDescription("column name for the predicted class of species")
  var predictionClassName: String = _

  @JsonProperty(
    value = "prediction probability name",
    required = true,
    defaultValue = "Species_probability"
  )
  @JsonPropertyDescription("column name for the prediction's probability of being a Iris-setosa")
  var predictionProbabilityName: String = _

  /**
    * This method is to be implemented to generate the actual Python source code
    * based on operators predicates.
    *
    * @return a String representation of the executable Python source code.
    */
  override def generatePythonCode(): String = {
    s"""from pytexera import *
       |import numpy as np
       |import torch
       |import torch.nn as nn
       |from datasets import load_dataset
       |from huggingface_hub import PyTorchModelHubMixin
       |from sklearn.model_selection import train_test_split
       |
       |iris = load_dataset("scikit-learn/iris")
       |iris.set_format("pandas")
       |iris_df = iris['train'][:]
       |X = iris_df[['PetalLengthCm', 'PetalWidthCm']]
       |y = (iris_df['Species'] == "Iris-setosa").astype(int)
       |
       |class_names = ["Not Iris-setosa", "Iris-setosa"]
       |
       |X_train, X_val, y_train, y_val = train_test_split(X.values, y.values, test_size=0.3, stratify=y, random_state=42)
       |X_means, X_stds = X_train.mean(axis=0), X_train.std(axis=0)
       |
       |device = torch.device("cpu")
       |
       |class LinearModel(nn.Module, PyTorchModelHubMixin):
       |    def __init__(self):
       |        super().__init__()
       |        self.fc = nn.Linear(2, 1)
       |
       |    def forward(self, x):
       |        out = self.fc(x)
       |        return out
       |
       |model = LinearModel.from_pretrained("sadhaklal/logistic-regression-iris")
       |model.to(device)
       |
       |class ProcessTupleOperator(UDFOperatorV2):
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        length = tuple_["$petalLengthCmAttribute"]
       |        width = tuple_["$petalWidthCmAttribute"]
       |        X_new = np.array([[length, width]])
       |        X_new = ((X_new - X_means) / X_stds)
       |        X_new = torch.from_numpy(X_new).float()
       |        model.eval()
       |        X_new = X_new.to(device)
       |        with torch.no_grad():
       |            logits = model(X_new)
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
      OperatorGroupConstants.MACHINE_LEARNING_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    if (
      predictionClassName == null || predictionClassName.trim.isEmpty ||
      predictionProbabilityName == null || predictionProbabilityName.trim.isEmpty
    )
      return null
    Schema
      .builder()
      .add(schemas(0))
      .add(predictionClassName, AttributeType.STRING)
      .add(predictionProbabilityName, AttributeType.DOUBLE)
      .build()
  }
}
