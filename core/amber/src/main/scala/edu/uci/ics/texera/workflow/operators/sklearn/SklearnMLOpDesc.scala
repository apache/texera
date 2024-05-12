package edu.uci.ics.texera.workflow.operators.sklearn

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

abstract class SklearnMLOpDesc extends PythonOperatorDescriptor {

  @JsonIgnore
  var classification: Boolean = true

  @JsonProperty(value = "Target Attribute", required = true)
  @JsonPropertyDescription("attribute in your dataset corresponding to target")
  @AutofillAttributeName
  var target: String = _

  @JsonIgnore
  def getImportStatements = ""

  @JsonIgnore
  def getUserFriendlyModelName = ""

  override def generatePythonCode(): String =
    s"""$getImportStatements
       |from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, mean_absolute_error, r2_score
       |import numpy as np
       |from pytexera import *
       |class ProcessTableOperator(UDFTableOperator):
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        Y = table["$target"]
       |        X = table.drop("$target", axis=1)
       |        if port == 0:
       |            self.model = ${getImportStatements.split(" ").last}().fit(X, Y)
       |        else:
       |            predictions = self.model.predict(X)
       |            if ${if (classification) "True"
    else "False"}:
       |                accuracy = accuracy_score(Y, predictions)
       |                print("Overall Accuracy:", accuracy)
       |
       |                f1s = f1_score(Y, predictions, average=None)
       |                precisions = precision_score(Y, predictions, average=None)
       |                recalls = recall_score(Y, predictions, average=None)
       |                for i, class_name in enumerate(np.unique(Y)):
       |                    print("Class", repr(class_name), " - F1:", f1s[i], ", Precision:", precisions[i], ", Recall:", recalls[i])
       |                yield {"model_name" : "$getUserFriendlyModelName", "model" : self.model}
       |            else:
       |                mae = mean_absolute_error(Y, predictions)
       |                r2 = r2_score(Y, predictions)
       |                print("MAE:", mae, ", R2:", r2)
       |                yield {"model_name" : "$getUserFriendlyModelName", "model" : self.model}""".stripMargin

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      getUserFriendlyModelName,
      "Sklearn " + getUserFriendlyModelName + " Operator",
      OperatorGroupConstants.SKLEARN_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), "training"),
        InputPort(PortIdentity(1), "testing", dependencies = List(PortIdentity()))
      ),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema
      .builder()
      .add("model_name", AttributeType.STRING)
      .add("model", AttributeType.BINARY)
      .build()
  }
}
