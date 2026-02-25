/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList
}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
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
  var groundTruthAttribute: EncodableString = ""

  @JsonProperty(value = "Selected Features", required = true)
  @JsonSchemaTitle("Selected Features")
  @JsonPropertyDescription("Features used to train the model")
  @AutofillAttributeNameList
  var selectedFeatures: List[EncodableString] = _

  private def getLoopTimes(paraList: List[HyperParameters[T]]): PythonTemplateBuilder = {
    for (ele <- paraList) {
      if (ele.parametersSource) {
        return pyb"""table[${ele.attribute}].values.shape[0]"""
      }
    }
    pyb"1"
  }

  def getParameter(paraList: List[HyperParameters[T]]): List[PythonTemplateBuilder] = {
    var workflowParam = s"";
    var portParam = pyb"";
    var paramString = pyb""
    for (ele <- paraList) {
      if (ele.parametersSource) {
        workflowParam = s"$workflowParam${ele.parameter.getName} = {},"
        portParam =
          portParam + pyb"${ele.parameter.getType}(table[${ele.attribute}].values[i]),"
        paramString =
          pyb"$paramString${ele.parameter.getName} = ${ele.parameter.getType}(table[${ele.attribute}].values[i]),"
      } else {
        workflowParam = s"$workflowParam${ele.parameter.getName} = {},"
        portParam = pyb"$portParam${ele.parameter.getType} (${ele.value}),"
        paramString =
          pyb"$paramString${ele.parameter.getName} = ${ele.parameter.getType} (${ele.value}),"
      }
    }
    List(pyb""""$workflowParam".format($portParam)""", paramString)

  }

  override def generatePythonCode(): String = {
    val listFeatures = selectedFeatures.map(feature => pyb"""$feature""").mkString(",")
    val trainingName = getImportStatements.split(" ").last
    val stringList = getParameter(paraList)
    val trainingParam = stringList(1)
    val paramString = stringList(0)
    val finalCode =
      pyb"""
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
         |      y_train = self.dataset[$groundTruthAttribute]
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
         |"""
    finalCode.encode
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

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema(
      List(
        new Attribute("Model", AttributeType.BINARY),
        new Attribute("Parameters", AttributeType.STRING)
      )
    )

    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }
}
