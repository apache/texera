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

  @JsonProperty(value = "Target Attribute", required = true)
  @JsonPropertyDescription("attribute in your dataset corresponding to target")
  @AutofillAttributeName
  var target: String = _

  override def generatePythonCode(): String =
    s"""$model
       |from pytexera import *
       |from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
       |class ProcessTableOperator(UDFTableOperator):
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        if port == 0:
       |            self.model = ${model
      .split(" ")
      .last}().fit(table.drop("$target", axis=1), table["$target"])
       |        else:
       |            predictions = self.model.predict(table.drop("$target", axis=1))
       |            auc = accuracy_score(table["$target"], predictions)
       |            f1 = f1_score(table["$target"], predictions, average='micro')
       |            precision = precision_score(table["$target"], predictions, average='micro')
       |            recall = recall_score(table["$target"], predictions, average='micro')
       |            print("Accuracy:", auc)
       |            print("F1:", f1)
       |            print("Precision:", precision)
       |            print("Recall:", recall)
       |            yield {"name" : "$name",
       |                   "accuracy" : auc,
       |                   "f1" : f1,
       |                   "precision" : precision,
       |                   "recall" : recall,
       |                   "model" : self.model}""".stripMargin

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      name,
      "Skleanr " + name + " Operator",
      OperatorGroupConstants.MACHINE_LEARNING_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), "training"),
        InputPort(PortIdentity(1), "testing", dependencies = List(PortIdentity()))
      ),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema =
    Schema
      .builder()
      .add("name", AttributeType.STRING)
      .add("accuracy", AttributeType.DOUBLE)
      .add("f1", AttributeType.DOUBLE)
      .add("precision", AttributeType.DOUBLE)
      .add("recall", AttributeType.DOUBLE)
      .add("model", AttributeType.BINARY)
      .build()
}
