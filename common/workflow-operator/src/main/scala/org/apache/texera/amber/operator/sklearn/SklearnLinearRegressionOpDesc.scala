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
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.{AutofillAttributeName, SampleColumn}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral

class SklearnLinearRegressionOpDesc
    extends PythonOperatorDescriptor
    with StandaloneCodeGenerator
    with SklearnFittableColumns {

  @JsonSchemaTitle("Target Attribute")
  @JsonPropertyDescription("Attribute in your dataset corresponding to target.")
  @JsonProperty(required = true)
  @AutofillAttributeName
  // The label the estimator fits against. Test-only steering: without it the
  // first column wins, which on a feature/label table is a feature.
  @SampleColumn("species")
  var target: EncodableString = _

  @JsonSchemaTitle("Degree")
  @JsonPropertyDescription("Degree of polynomial function")
  @JsonProperty(required = true)
  val degree: Int = 1

  override def generatePythonCode(): String =
    pyb"""
       |from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, mean_absolute_error, r2_score
       |from sklearn.pipeline import make_pipeline
       |from sklearn.linear_model import LinearRegression
       |from sklearn.preprocessing import PolynomialFeatures
       |import numpy as np
       |from pytexera import *
       |class ProcessTableOperator(UDFTableOperator):
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        rows_read = len(table)
       |        table = table.dropna() #remove missing values
       |        if len(table) < rows_read:
       |            print("Skipped", rows_read - len(table), "of", rows_read, "rows with missing values")
       |        Y = table[$target]
       |        X = table.drop($target, axis=1)
       |${narrowToFittableColumns("X", " " * 8)}
       |        if port == 0:
       |            pipeline = make_pipeline(
       |                PolynomialFeatures(degree=$degree),
       |                LinearRegression()
       |            )
       |            self.model = pipeline.fit(X, Y)
       |        else:
       |            predictions = self.model.predict(X)
       |            mae = round(mean_absolute_error(Y, predictions), 4)
       |            r2 = round(r2_score(Y, predictions), 4)
       |            print("MAE:", mae, ", R2:", r2)
       |            yield {"model_name" : "LinearRegression", "model" : self.model}""".encode

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Linear Regression",
      "Sklearn Linear Regression Operator",
      OperatorGroupConstants.SKLEARN_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), "training"),
        InputPort(PortIdentity(1), "testing", dependencies = List(PortIdentity()))
      ),
      outputPorts = List(OutputPort(blocking = true))
    )

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    Map(
      operatorInfo.outputPorts.head.id -> Schema()
        .add("model_name", AttributeType.STRING)
        .add("model", AttributeType.BINARY)
    )
  }

  override def generateStandaloneCode(): String = {
    val targetLit = pyStringLiteral(target)
    s"""from sklearn.metrics import mean_absolute_error, r2_score
       |from sklearn.pipeline import make_pipeline
       |from sklearn.linear_model import LinearRegression
       |from sklearn.preprocessing import PolynomialFeatures
       |import pandas as pd
       |
       |# The same rows the operator drops, and on both frames: the model is fitted
       |# on one and scored on the other, so narrowing only one side would fit and
       |# score on different data. Local names rather than reassignments, since the
       |# input variables belong to whichever operators produced them.
       |_train = in1df.dropna()
       |if len(_train) < len(in1df):
       |    print("Skipped", len(in1df) - len(_train), "of", len(in1df), "rows with missing values")
       |Y_train = _train[$targetLit]
       |X_train = _train.drop($targetLit, axis=1)
       |${narrowToFittableColumns("X_train", "")}
       |pipeline = make_pipeline(
       |    PolynomialFeatures(degree=$degree),
       |    LinearRegression()
       |)
       |model = pipeline.fit(X_train, Y_train)
       |
       |_test = in2df.dropna()
       |Y_test = _test[$targetLit]
       |X_test = _test.drop($targetLit, axis=1)
       |${narrowToFittableColumns("X_test", "")}
       |predictions = model.predict(X_test)
       |mae = round(mean_absolute_error(Y_test, predictions), 4)
       |r2 = round(r2_score(Y_test, predictions), 4)
       |print("MAE:", mae, ", R2:", r2)
       |out1df = pd.DataFrame([{"model_name": "LinearRegression", "model": model}])""".stripMargin
  }

}
