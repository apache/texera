package edu.uci.ics.texera.workflow.operators.sklearn

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

abstract class SklearnOp extends PythonOperatorDescriptor {
  @JsonProperty(value = "target", required = true)
  @JsonPropertyDescription("column in your dataset corresponding to target")
  @AutofillAttributeName
  var target: String = _

  def model: String = s"""from sklearn.dummy import DummyClassifier
       |model = DummyClassifier()""".stripMargin

  def operatorName = "Dummy Classifier"

  override def generatePythonCode(): String = model +
    s"""
       |from pytexera import *
       |from sklearn.metrics import accuracy_score
       |class ProcessTableOperator(UDFTableOperator):
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        if port == 0:
       |            self.model = model.fit(table.drop("$target", axis=1), table["$target"])
       |        else:
       |            auc = predictions = self.model.predict(table.drop("$target", axis=1))
       |            print("Accuracy:", auc)
       |            yield {"name" : $operatorName,
       |                   "accuracy" : auc,
       |                   "model" : self.model}""".stripMargin


  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      operatorName,
      "Skleanr " + operatorName + " Operator",
      OperatorGroupConstants.MACHINE_LEARNING_GROUP,
      inputPorts = List(InputPort(PortIdentity(), "training"),InputPort(PortIdentity(1), "testing",dependencies = List(PortIdentity()))),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema
      .builder()
      .add("name", AttributeType.STRING)
      .add("accuracy", AttributeType.DOUBLE)
      .add("model", AttributeType.BINARY)
      .build()
  }
}