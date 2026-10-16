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

package org.apache.texera.amber.operator.sklearn.training

import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.sklearn.SklearnModelOpDesc

class SklearnTrainingOpDesc extends SklearnModelOpDesc with StandaloneCodeGenerator {

  override def getImportStatements = ""

  override def getUserFriendlyModelName = ""

  override def generatePythonCode(): String =
    pyb"""$getImportStatements
       |from sklearn.pipeline import make_pipeline
       |from sklearn.compose import ColumnTransformer
       |from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
       |import numpy as np
       |from pytexera import *
       |class ProcessTableOperator(UDFTableOperator):
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        rows_read = len(table)
       |        table = $dropMissingRows #remove missing values
       |        if len(table) < rows_read:
       |            print("Skipped", rows_read - len(table), "of", rows_read, "rows with missing values")
       |        Y = table[$target]
       |        X = table.drop($target, axis=1)
       |${dropNonFeatureColumns("X", " " * 8)}
$reportMissingKept
       |        model = make_pipeline(${vectorizerStage(c => pyb"$c".toString)} ${if (
      tfidfTransformer
    ) "TfidfTransformer(),"
    else ""} ${getImportStatements.split(" ").last}()).fit(X, Y)
       |        yield {"model_name" : "$getUserFriendlyModelName", "model" : model}
       |
       |        """.encode

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      getUserFriendlyModelName,
      "Sklearn " + getUserFriendlyModelName + " Operator",
      OperatorGroupConstants.SKLEARN_TRAINING_GROUP,
      inputPorts = List(InputPort(PortIdentity(), "training")),
      outputPorts = List(OutputPort(blocking = true))
    )

  override def generateStandaloneCode(): String = {
    val estimator = getImportStatements.split(" ").last
    val tfidfPart = if (tfidfTransformer) "TfidfTransformer()," else ""
    val targetLit = pyStringLiteral(target)
    val modelNameLit = pyStringLiteral(getUserFriendlyModelName)
    val narrowX = dropNonFeatureColumns("X", "")

    s"""${getImportStatements}
       |from sklearn.pipeline import make_pipeline
       |from sklearn.compose import ColumnTransformer
       |from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
       |import pandas as pd
       |
       |# The same rows the operator drops. A local name rather than a
       |# reassignment, since the input variable belongs to whichever operator
       |# produced it.
       |_train = ${dropMissingRowsStandalone("in1df")}
       |if len(_train) < len(in1df):
       |    print("Skipped", len(in1df) - len(_train), "of", len(in1df), "rows with missing values")
       |Y = _train[$targetLit]
       |X = _train.drop($targetLit, axis=1)
       |$narrowX
       |model = make_pipeline(${vectorizerStage(c => pyStringLiteral(c))}$tfidfPart$estimator()).fit(X, Y)
       |out1df = pd.DataFrame([{"model_name": $modelNameLit, "model": model}])""".stripMargin
  }
}
