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

package org.apache.texera.amber.operator.sklearn

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameOnPort1
}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral

class SklearnPredictionOpDesc extends PythonOperatorDescriptor with StandaloneCodeGenerator {
  @JsonProperty(value = "Model Attribute", required = true, defaultValue = "model")
  @JsonPropertyDescription("attribute corresponding to ML model")
  @AutofillAttributeName
  var model: EncodableString = _

  @JsonProperty(value = "Output Attribute Name", required = true, defaultValue = "prediction")
  @JsonPropertyDescription("attribute name of the prediction result")
  var resultAttribute: EncodableString = _

  @JsonProperty(
    value = "Ground Truth Attribute Name to Ignore",
    required = false,
    defaultValue = ""
  )
  @JsonPropertyDescription("attribute name of the ground truth")
  @AutofillAttributeNameOnPort1
  var groundTruthAttribute: EncodableString = ""

  override def generatePythonCode(): String =
    pyb"""from pytexera import *
       |from sklearn.pipeline import Pipeline
       |class ProcessTupleOperator(UDFOperatorV2):
       |    @overrides
       |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
       |        if port == 0:
       |            self.model = tuple_[$model]
       |        else:
       |            input_features = tuple_
       |            if $groundTruthAttribute != "":
       |                input_features = input_features.get_partial_tuple([col for col in tuple_.get_field_names() if col != $groundTruthAttribute])
       |            _fitted = getattr(self.model, "feature_names_in_", None)
       |            if _fitted is not None:
       |                input_features = input_features.get_partial_tuple(list(_fitted))
       |            if Table.from_tuple_likes([input_features]).isna().any(axis=None):
       |                tuple_[$resultAttribute] = None #keep the row, leave the result empty
       |            else:
       |                prediction = self.model.predict(Table.from_tuple_likes([input_features]))[0]
       |                #the output schema names this column's type, so reading one off a row could only disagree with it
       |                tuple_[$resultAttribute] = prediction if $groundTruthAttribute != "" else str(prediction)
       |            yield tuple_""".encode

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sklearn Prediction",
      "Sklearn Prediction Operator",
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
      operatorInfo.outputPorts.head.id -> inputSchema
        .add(resultAttribute, resultType)
    )
  }

  /** Python that narrows `X` to the columns the model was fitted on.
    *
    * The fitting side leaves out the columns an estimator cannot fit, so this
    * side has to leave out the same ones or scikit-learn refuses the frame for
    * naming features it never saw. Read off the model rather than re-derived:
    * what it was fitted on is a fact it carries, and asking it cannot drift from
    * whatever rule the fitting operator applied.
    */
  private val narrowToFittedFeatures: String =
    """_fitted = getattr(model, "feature_names_in_", None)
      |if _fitted is not None:
      |    X = X[list(_fitted)]""".stripMargin

  override def generateStandaloneCode(): String = {
    val modelLit = pyStringLiteral(model)
    val resultLit = pyStringLiteral(resultAttribute)
    if (groundTruthAttribute.nonEmpty) {
      s"""from sklearn.pipeline import Pipeline
         |
         |model = in1df[$modelLit].iloc[0]
         |out1df = in2df.copy()
         |X = in2df.drop(${pyStringLiteral(groundTruthAttribute)}, axis=1)
         |$narrowToFittedFeatures
         |out1df[$resultLit] = model.predict(X)""".stripMargin
    } else {
      s"""from sklearn.pipeline import Pipeline
         |
         |model = in1df[$modelLit].iloc[0]
         |out1df = in2df.copy()
         |X = in2df
         |$narrowToFittedFeatures
         |out1df[$resultLit] = [str(p) for p in model.predict(X)]""".stripMargin
    }
  }
}
