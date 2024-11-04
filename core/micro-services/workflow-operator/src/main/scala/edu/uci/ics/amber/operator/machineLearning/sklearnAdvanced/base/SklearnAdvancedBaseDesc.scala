package edu.uci.ics.amber.operator.machineLearning.sklearnAdvanced.base

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.OperatorInfo
import edu.uci.ics.amber.operator.metadata.OperatorGroupConstants
import edu.uci.ics.amber.operator.metadata.annotation.{
  AutofillAttributeName,
  AutofillAttributeNameList
}
import edu.uci.ics.amber.workflow.{InputPort, OutputPort, PortIdentity}

trait ParamClass {
  def getName: String
  def getType: String
}

abstract class SklearnMLOperatorDescriptor[T <: ParamClass] extends PythonOperatorDescriptor {
  @JsonIgnore
  def getImportStatements: String

  @JsonIgnore
  def getOperatorInfo: String

  @JsonProperty(required = true)
  @JsonSchemaTitle("Parameter Setting")
  var paraList: List[HyperParameters[T]] = List()

  @JsonProperty(required = true)
  @JsonSchemaTitle("Ground Truth Attribute Column")
  @JsonPropertyDescription("Ground truth attribute column")
  @AutofillAttributeName
  var groundTruthAttribute: String = ""

  @JsonProperty(value = "Selected Features", required = true)
  @JsonSchemaTitle("Selected Features")
  @JsonPropertyDescription("Features used to train the model")
  @AutofillAttributeNameList
  var selectedFeatures: List[String] = _

  private def getLoopTimes(paraList: List[HyperParameters[T]]): String = {
    for (ele <- paraList) {
      if (ele.parametersSource) {
        return s"""table[\"${ele.attribute}\"].values.shape[0]"""
      }
    }
    "1"
  }

  def getParameter(paraList: List[HyperParameters[T]]): List[String] = {
    var workflowParam = ""; var portParam = ""; var paramString = ""
    for (ele <- paraList) {
      if (ele.parametersSource) {
        workflowParam = workflowParam + String.format("%s = {},", ele.parameter.getName)
        portParam =
          portParam + String.format(
            "%s(table['%s'].values[i]),",
            ele.parameter.getType,
            ele.attribute
          )
        paramString = paramString + String.format(
          "%s = %s(table['%s'].values[i]),",
          ele.parameter.getName,
          ele.parameter.getType,
          ele.attribute
        )
      } else {
        workflowParam = workflowParam + String.format("%s = {},", ele.parameter.getName)
        portParam = portParam + String.format("%s ('%s'),", ele.parameter.getType, ele.value)
        paramString = paramString + String.format(
          "%s = %s ('%s'),",
          ele.parameter.getName,
          ele.parameter.getType,
          ele.value
        )
      }
    }
    List(String.format("\"%s\".format(%s)", workflowParam, portParam), paramString)
  }

  override def generatePythonCode(): String = {
    val listFeatures = selectedFeatures.map(feature => s""""$feature"""").mkString(",")
    val trainingName = getImportStatements.split(" ").last
    val stringList = getParameter(paraList)
    val trainingParam = stringList(1)
    val paramString = stringList(0)
    val finalCode =
      s"""
         |from pytexera import *
         |
         |import pandas as pd
         |${getImportStatements}
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |  @overrides
         |  def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |    model_list = []
         |    para_list = []
         |    features = [$listFeatures]
         |
         |    if port == 0:
         |      self.dataset = table
         |
         |    if port == 1 :
         |      y_train = self.dataset["$groundTruthAttribute"]
         |      X_train = self.dataset[features]
         |      loop_times = ${getLoopTimes(paraList)}
         |
         |      for i in range(loop_times):
         |        model = ${trainingName}(${trainingParam})
         |        model.fit(X_train, y_train)
         |        model_list.append(model)
         |        para_str = ${paramString}
         |        para_list.append(para_str)
         |
         |      data = dict()
         |      data["Model"]= model_list
         |      data["Parameters"] =para_list
         |
         |      df = pd.DataFrame(data)
         |      yield df
         |
         |""".stripMargin
    finalCode
  }

  override def operatorInfo: OperatorInfo = {
    val name = getOperatorInfo
    OperatorInfo(
      name,
      "Sklearn " + name + " Operator",
      OperatorGroupConstants.ADVANCED_SKLEARN_GROUP,
      inputPorts = List(
        InputPort(
          PortIdentity(0),
          displayName = "training"
        ),
        InputPort(
          PortIdentity(1),
          displayName = "parameter",
          dependencies = List(PortIdentity(0))
        )
      ),
      outputPorts = List(OutputPort())
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    val outputSchemaBuilder = Schema.builder()
    outputSchemaBuilder.add(new Attribute("Model", AttributeType.BINARY))
    outputSchemaBuilder.add(new Attribute("Parameters", AttributeType.STRING)).build()
  }
}
