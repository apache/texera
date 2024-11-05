package edu.uci.ics.amber.operator.sklearn

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.OperatorInfo
import edu.uci.ics.amber.operator.metadata.OperatorGroupConstants
import edu.uci.ics.amber.operator.metadata.annotation.AutofillAttributeName
import edu.uci.ics.amber.workflow.{InputPort, OutputPort, PortIdentity}

class SklearnPredictionOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(value = "Model Attribute", required = true, defaultValue = "model")
  @JsonPropertyDescription("attribute corresponding to ML model")
  @AutofillAttributeName
  var model: String = _

  @JsonProperty(value = "Output Attribute Name", required = true, defaultValue = "prediction")
  @JsonPropertyDescription("attribute name of the prediction result")
  var resultAttribute: String = _

  override def generatePythonCode(): String =
    s"""from pytexera import *
       |from sklearn.pipeline import Pipeline
       |class ProcessTupleOperator(UDFOperatorV2):
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        if port == 0:
       |            self.model = tuple_["$model"]
       |        else:
       |            tuple_["$resultAttribute"] = str(self.model.predict(Table.from_tuple_likes([tuple_]))[0])
       |            yield tuple_""".stripMargin

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sklearn Prediction",
      "Skleanr Prediction Operator",
      OperatorGroupConstants.SKLEARN_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), "model"),
        InputPort(PortIdentity(1), "testing", dependencies = List(PortIdentity()))
      ),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema =
    Schema
      .builder()
      .add(schemas(1))
      .add(resultAttribute, AttributeType.STRING)
      .build()
}
