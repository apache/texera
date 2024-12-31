package edu.uci.ics.amber.operator.sklearn

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameOnPort1
}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}

class SklearnPredictionOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(value = "Model Attribute", required = true, defaultValue = "model")
  @JsonPropertyDescription("attribute corresponding to ML model")
  @AutofillAttributeName
  var model: String = _

  @JsonProperty(value = "Output Attribute Name", required = true, defaultValue = "prediction")
  @JsonPropertyDescription("attribute name of the prediction result")
  var resultAttribute: String = _

  @JsonProperty(
    value = "Ground Truth Attribute Name to Ignore",
    required = false,
    defaultValue = ""
  )
  @JsonPropertyDescription("attribute name of the ground truth")
  @AutofillAttributeNameOnPort1
  var groundTruthAttribute: String = ""

  override def generatePythonCode(): String =
    s"""from pytexera import *
       |from sklearn.pipeline import Pipeline
       |class ProcessTupleOperator(UDFOperatorV2):
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        if port == 0:
       |            self.model = tuple_["$model"]
       |        else:
       |            input_features = tuple_
       |            if "$groundTruthAttribute" != "":
       |                input_features = input_features.get_partial_tuple([col for col in tuple_.get_field_names() if col != "$groundTruthAttribute"])
       |                tuple_["$resultAttribute"] = type(tuple_["$groundTruthAttribute"])(self.model.predict(Table.from_tuple_likes([input_features]))[0])
       |            else:
       |                tuple_["$resultAttribute"] = str(self.model.predict(Table.from_tuple_likes([input_features]))[0])
       |            yield tuple_""".stripMargin

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sklearn Prediction",
      "Skleanr Prediction Operator",
      OperatorGroupConstants.SKLEARN_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), "model"),
        InputPort(PortIdentity(1), dependencies = List(PortIdentity()))
      ),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    var resultType = AttributeType.STRING
    val inputSchema = inputSchemas(operatorInfo.inputPorts(1).id)
    if (groundTruthAttribute != "") {
      resultType =
        inputSchema.attributes.find(attr => attr.getName == groundTruthAttribute).get.getType
    }
    Map(
      operatorInfo.outputPorts.head.id -> Schema
        .builder()
        .add(inputSchema)
        .add(resultAttribute, resultType)
        .build()
    )
  }
}
