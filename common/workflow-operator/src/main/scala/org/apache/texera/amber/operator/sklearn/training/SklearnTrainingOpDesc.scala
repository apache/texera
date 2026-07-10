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

import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.sklearn.SklearnModelOpDesc

class SklearnTrainingOpDesc extends SklearnModelOpDesc {

  override def getImportStatements = ""

  override def getUserFriendlyModelName = ""

  override def generatePythonCode(): String =
    pyb"""$getImportStatements
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
       |        model = make_pipeline(${if (countVectorizer) "CountVectorizer()," else ""} ${if (
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
}
