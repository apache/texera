package edu.uci.ics.texera.workflow.operators.sklearn

import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

class SklearnPredictionOp extends PythonOperatorDescriptor {
  override def generatePythonCode(): String =
    s"""from pytexera import *
       |class ProcessTupleOperator(UDFOperatorV2):
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        if port == 0:
       |            self.model = tuple_["model"]
       |        else:
       |            tuple_["prediction"] = str(self.model.predict([tuple_])[0])
       |            yield tuple_""".stripMargin

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sklearn Prediction",
      "Skleanr Prediction Operator",
      OperatorGroupConstants.MACHINE_LEARNING_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), "model"),
        InputPort(PortIdentity(1), "testing", dependencies = List(PortIdentity()))
      ),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema =
    Schema
      .builder()
      .add(schemas(1))
      .add("prediction", AttributeType.STRING)
      .build()
}
