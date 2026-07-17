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

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaInt,
  JsonSchemaString,
  JsonSchemaTitle
}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  CommonOpDescAnnotation,
  HideAnnotation
}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

abstract class SklearnClassifierOpDesc
    extends PythonOperatorDescriptor
    with StandaloneCodeGenerator {

  @JsonSchemaTitle("Target Attribute")
  @JsonPropertyDescription("Attribute in your dataset corresponding to target.")
  @JsonProperty(required = true)
  @AutofillAttributeName
  var target: EncodableString = _

  @JsonSchemaTitle("Count Vectorizer")
  @JsonPropertyDescription("Convert a collection of text documents to a matrix of token counts.")
  @JsonProperty(defaultValue = "false")
  var countVectorizer: Boolean = false

  @JsonSchemaTitle("Text Attribute")
  @JsonPropertyDescription("Attribute in your dataset with text to vectorize.")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(
        path = CommonOpDescAnnotation.autofill,
        value = CommonOpDescAnnotation.attributeName
      ),
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "countVectorizer"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    ),
    ints = Array(
      new JsonSchemaInt(path = CommonOpDescAnnotation.autofillAttributeOnPort, value = 0)
    )
  )
  var text: EncodableString = _

  @JsonSchemaTitle("Tfidf Transformer")
  @JsonPropertyDescription("Transform a count matrix to a normalized tf or tf-idf representation.")
  @JsonProperty(defaultValue = "false")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "countVectorizer"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    )
  )
  val tfidfTransformer: Boolean = false

  @JsonIgnore
  def getImportStatements = ""

  @JsonIgnore
  def getUserFriendlyModelName = ""

  override def generatePythonCode(): String =
    pyb"""$getImportStatements
       |from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
       |from sklearn.pipeline import make_pipeline
       |from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
       |import numpy as np
       |from pytexera import *
       |class ProcessTableOperator(UDFTableOperator):
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        Y = table[$target]
       |        X = table.drop($target, axis=1)
       |        X = ${if (countVectorizer) pyb"X[$text]" else "X"}
       |        if port == 0:
       |            self.model = make_pipeline(${if (countVectorizer) "CountVectorizer(),"
    else ""} ${if (tfidfTransformer) "TfidfTransformer()," else ""} ${getImportStatements
      .split(" ")
      .last}()).fit(X, Y)
       |        else:
       |            predictions = self.model.predict(X)
       |            print("Overall Accuracy:", round(accuracy_score(Y, predictions), 4))
       |            f1s = f1_score(Y, predictions, average=None)
       |            precisions = precision_score(Y, predictions, average=None)
       |            recalls = recall_score(Y, predictions, average=None)
       |            for i, class_name in enumerate(np.unique(Y)):
       |                print("Class", repr(class_name), " - F1:", round(f1s[i], 4), ", Precision:", round(precisions[i], 4), ", Recall:", round(recalls[i], 4))
       |            yield {"model_name" : "$getUserFriendlyModelName", "model" : self.model}""".encode

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      getUserFriendlyModelName,
      "Sklearn " + getUserFriendlyModelName + " Operator",
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
    val estimator = getImportStatements.split(" ").last
    val cvPart = if (countVectorizer) "CountVectorizer()," else ""
    val tfidfPart = if (tfidfTransformer) "TfidfTransformer()," else ""
    val trainX =
      if (countVectorizer) s"""in1df["$text"]""" else s"""in1df.drop("$target", axis=1)"""
    val testX =
      if (countVectorizer) s"""in2df["$text"]""" else s"""in2df.drop("$target", axis=1)"""

    s"""${getImportStatements}
       |from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
       |from sklearn.pipeline import make_pipeline
       |from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
       |import numpy as np
       |import pandas as pd
       |
       |Y_train = in1df["$target"]
       |X_train = $trainX
       |model = make_pipeline($cvPart$tfidfPart$estimator()).fit(X_train, Y_train)
       |
       |Y_test = in2df["$target"]
       |X_test = $testX
       |predictions = model.predict(X_test)
       |print("Overall Accuracy:", round(accuracy_score(Y_test, predictions), 4))
       |f1s = f1_score(Y_test, predictions, average=None)
       |precisions = precision_score(Y_test, predictions, average=None)
       |recalls = recall_score(Y_test, predictions, average=None)
       |for i, class_name in enumerate(np.unique(Y_test)):
       |    print("Class", repr(class_name), " - F1:", round(f1s[i], 4), ", Precision:", round(precisions[i], 4), ", Recall:", round(recalls[i], 4))
       |out1df = pd.DataFrame([{"model_name": "${getUserFriendlyModelName}", "model": model}])""".stripMargin
  }
}
