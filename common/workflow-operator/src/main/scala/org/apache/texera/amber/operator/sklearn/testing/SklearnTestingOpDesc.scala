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

package org.apache.texera.amber.operator.sklearn.testing

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.sklearn.SklearnFittableColumns
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameOnPort1
}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{
  PythonTemplateBuilderStringContext,
  pyStringLiteral
}

class SklearnTestingOpDesc
    extends PythonOperatorDescriptor
    with StandaloneCodeGenerator
    with SklearnFittableColumns {
  @JsonProperty(required = true, defaultValue = "false")
  @JsonSchemaTitle("Regression")
  @JsonPropertyDescription(
    "Choose to solve a regression task"
  )
  var isRegression: Boolean = false

  @JsonSchemaTitle("Model Attribute")
  @JsonProperty(required = true, defaultValue = "model")
  @JsonPropertyDescription("Attribute corresponding to ML model")
  @AutofillAttributeName
  var model: EncodableString = _

  @JsonSchemaTitle("Target Attribute")
  @JsonPropertyDescription("Attribute in your dataset corresponding to target.")
  @JsonProperty(required = true)
  @AutofillAttributeNameOnPort1
  var target: EncodableString = _

  override def generatePythonCode(): String = {
    val isRegressionStr = if (isRegression) "True" else "False"
    pyb"""from pytexera import *
         |from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, root_mean_squared_error, mean_absolute_error, r2_score
         |class ProcessTupleOperator(UDFOperatorV2):
         |    @overrides
         |    def open(self) -> None:
         |        self.data = []
         |    @overrides
         |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
         |        if port == 1:
         |            self.data.append(tuple_)
         |        else:
         |            model = tuple_[$model]
         |            #the model arrives already fitted, so this operator cannot ask which
         |            #estimator it holds and drops on every column to be safe
         |            rows_read = len(self.data)
         |            table = Table(self.data).dropna() #remove missing values
         |            if len(table) < rows_read:
         |                print("Skipped", rows_read - len(table), "of", rows_read, "rows with missing values")
         |            Y = table[$target]
         |            X = table.drop($target, axis=1)
         |${narrowToFittableColumns("X", " " * 12)}
         |            predictions = model.predict(X.squeeze())
         |            if $isRegressionStr:
         |                tuple_["R2"] = r2_score(Y, predictions)
         |                tuple_["RMSE"] = root_mean_squared_error(Y, predictions)
         |                tuple_["MAE"] = mean_absolute_error(Y, predictions)
         |            else:
         |                tuple_["accuracy"] = round(accuracy_score(Y, predictions), 4)
         |                tuple_["f1"] = f1_score(Y, predictions, average="weighted")
         |                tuple_["precision"] = precision_score(Y, predictions, average="weighted")
         |                tuple_["recall"] = recall_score(Y, predictions, average="weighted")
         |            yield tuple_""".encode
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sklearn Testing",
      "It will generate scorers for Sklearn model",
      OperatorGroupConstants.SKLEARN_GROUP,
      inputPorts = List(
        InputPort(
          PortIdentity(),
          "model",
          dependencies = List(PortIdentity(1))
        ),
        InputPort(PortIdentity(1), "data")
      ),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] =
    Map(
      operatorInfo.outputPorts.head.id ->
        (if (!isRegression)
           Seq("accuracy", "f1", "precision", "recall")
         else
           Seq("R2", "RMSE", "MAE"))
          .foldLeft(inputSchemas(operatorInfo.inputPorts.head.id))(
            _.add(_, AttributeType.DOUBLE)
          )
    )

  override def generateStandaloneCode(): String = {
    val isRegressionStr = if (isRegression) "True" else "False"
    val modelLit = pyStringLiteral(model)
    val targetLit = pyStringLiteral(target)
    s"""from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, root_mean_squared_error, mean_absolute_error, r2_score
       |
       |model = in1df[$modelLit].iloc[0]
       |out1df = in1df.copy()
       |# The same drop the executor makes before it scores: the model arrives
       |# already fitted, so this operator cannot ask which estimator it holds and
       |# drops on every column to be safe. Scoring the rows it skips would answer
       |# with a number the run never reported, or fail inside scikit-learn.
       |rows_read = len(in2df)
       |scored_df = in2df.dropna()
       |if len(scored_df) < rows_read:
       |    print("Skipped", rows_read - len(scored_df), "of", rows_read, "rows with missing values")
       |Y = scored_df[$targetLit]
       |X = scored_df.drop($targetLit, axis=1)
       |${narrowToFittableColumns("X", "")}
       |predictions = model.predict(X.squeeze())
       |if $isRegressionStr:
       |    out1df["R2"] = r2_score(Y, predictions)
       |    out1df["RMSE"] = root_mean_squared_error(Y, predictions)
       |    out1df["MAE"] = mean_absolute_error(Y, predictions)
       |else:
       |    out1df["accuracy"] = round(accuracy_score(Y, predictions), 4)
       |    out1df["f1"] = f1_score(Y, predictions, average="weighted")
       |    out1df["precision"] = precision_score(Y, predictions, average="weighted")
       |    out1df["recall"] = recall_score(Y, predictions, average="weighted")""".stripMargin
  }

}
