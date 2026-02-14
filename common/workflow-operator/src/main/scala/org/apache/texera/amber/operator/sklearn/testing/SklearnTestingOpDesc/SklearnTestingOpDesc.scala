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

package org.apache.texera.amber.operator.sklearn.testing.SklearnTestingOpDesc

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameOnPort1
}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

class SklearnTestingOpDesc extends PythonOperatorDescriptor {
  @JsonSchemaTitle("Model Attribute")
  @JsonProperty(required = true, defaultValue = "model")
  @JsonPropertyDescription("Attribute corresponding to ML model")
  @AutofillAttributeNameOnPort1
  var model: String = _

  @JsonSchemaTitle("Target Attribute")
  @JsonPropertyDescription("Attribute in your dataset corresponding to target.")
  @JsonProperty(required = true)
  @AutofillAttributeName
  var target: String = _

  override def generatePythonCode(): String =
    s"""from pytexera import *
       |from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
       |class ProcessTupleOperator(UDFOperatorV2):
       |    @overrides
       |    def open(self) -> None:
       |        self.data = []
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        if port == 0:
       |            self.data.append(tuple_)
       |        else:
       |            model = tuple_["$model"]
       |            table = Table(self.data)
       |            Y = table["$target"]
       |            X = table.drop("$target", axis=1)
       |            predictions = model.predict(X)
       |            tuple_["accuracy"] = round(accuracy_score(Y, predictions), 4)
       |            tuple_["f1"] = f1_score(Y, predictions)
       |            tuple_["precision"] = precision_score(Y, predictions)
       |            tuple_["recall"] = recall_score(Y, predictions)
       |            yield tuple_""".stripMargin

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sklearn Testing",
      "It will generate F1, precision, accuracy and recall for a trained Sklearn model",
      OperatorGroupConstants.SKLEARN_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), "data"),
        InputPort(
          PortIdentity(1),
          "model",
          dependencies = List(PortIdentity()),
          allowMultiLinks = true
        )
      ),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val inputSchema = inputSchemas(operatorInfo.inputPorts(1).id)
    Map(
      operatorInfo.outputPorts.head.id -> inputSchema
        .add("accuracy", AttributeType.DOUBLE)
        .add("f1", AttributeType.DOUBLE)
        .add("precision", AttributeType.DOUBLE)
        .add("recall", AttributeType.DOUBLE)
    )
  }
}
