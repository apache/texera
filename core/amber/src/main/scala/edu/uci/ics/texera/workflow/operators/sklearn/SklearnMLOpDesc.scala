package edu.uci.ics.texera.workflow.operators.sklearn

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

abstract class SklearnMLOpDesc extends PythonOperatorDescriptor {

  @JsonIgnore
  var model = ""

  @JsonIgnore
  var name = ""

  @JsonIgnore
  var classification: Boolean = true

  @JsonProperty(value = "Target Attribute", required = true)
  @JsonPropertyDescription("attribute in your dataset corresponding to target")
  @AutofillAttributeName
  var target: String = _

  override def generatePythonCode(): String =
    s"""$model
       |from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, mean_absolute_error, r2_score
       |import pandas as pd
       |from pytexera import *
       |class ProcessTableOperator(UDFTableOperator):
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        if port == 0:
       |            self.model = ${model
      .split(" ")
      .last}().fit(table.drop("$target", axis=1), table["$target"])
       |        else:
       |            predictions = self.model.predict(table.drop("$target", axis=1))
       |            if ${if (classification) "True" else "False"}:
       |                auc = accuracy_score(table["$target"], predictions)
       |                f1 = f1_score(table["$target"], predictions, average='micro')
       |                precision = precision_score(table["$target"], predictions, average='micro')
       |                recall = recall_score(table["$target"], predictions, average='micro')
       |                print("Accuracy:", auc, ", F1:", f1, ", Precision:", precision, ", Recall:", recall)
       |                yield {"name" : "$name",
       |                   "accuracy" : auc,
       |                   "f1" : f1,
       |                   "precision" : precision,
       |                   "recall" : recall,
       |                   "model" : self.model}
       |            else:
       |                mae = mean_absolute_error(table["$target"], predictions)
       |                r2 = r2_score(table["$target"], predictions)
       |                print("MAE:", mae, ", R2:", r2)
       |                yield {"name" : "$name",
       |                  "mae": mae,
       |                  "r2": r2,
       |                  "model" : self.model}
       |                   """.stripMargin

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      name,
      "Sklearn " + name + " Operator",
      OperatorGroupConstants.SKLEARN_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), "training"),
        InputPort(PortIdentity(1), "testing", dependencies = List(PortIdentity()))
      ),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    val builder = Schema
      .builder()
      .add("name", AttributeType.STRING)
    if (classification) {
      builder
        .add("accuracy", AttributeType.DOUBLE)
        .add("f1", AttributeType.DOUBLE)
        .add("precision", AttributeType.DOUBLE)
        .add("recall", AttributeType.DOUBLE)
    } else {
      builder
        .add("mae", AttributeType.DOUBLE)
        .add("r2", AttributeType.DOUBLE)
    }
    builder.add("model", AttributeType.BINARY).build()
  }
}
