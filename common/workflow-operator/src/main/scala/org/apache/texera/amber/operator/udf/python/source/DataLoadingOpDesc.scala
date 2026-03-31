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

package org.apache.texera.amber.operator.udf.python.source

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{OutputPort, PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.operator.PythonCodeValidator

import scala.util.matching.Regex

/**
  * DataLoadingOp operator for loading data from external sources.
  *
  * This operator allows users to write a simple Python function that loads and returns data.
  * File I/O is allowed in this operator since it's a source operator.
  *
  * Example user code:
  * {{{
  * def load() -> pd.DataFrame:
  *     return pd.read_csv("/path/to/data.csv", nrows=100)
  * }}}
  *
  * Generated code:
  * {{{
  * from pytexera import *
  * import pandas as pd
  *
  * class GenerateOperator(UDFSourceOperator):
  *     @overrides
  *     def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
  *         yield load()
  *
  * def load() -> pd.DataFrame:
  *     return pd.read_csv("/path/to/data.csv", nrows=100)
  * }}}
  */
class DataLoadingOpDesc extends SourceOperatorDescriptor {

  @JsonProperty(
    required = true,
    defaultValue =
      "def load() -> pd.DataFrame:\n" +
        "    # Load data from file or external source\n" +
        "    # File IO is allowed in this operator\n" +
        "    # Return a DataFrame or dict\n" +
        "    return pd.DataFrame()\n"
  )
  @JsonSchemaTitle("Python function")
  @JsonPropertyDescription("input your code here")
  var code: String = _

  /**
    * Parses the function name from the code.
    * Defaults to "load" if not found.
    */
  private def parseFunctionName(): String = {
    val funcPattern: Regex = """def\s+(\w+)\s*\(""".r
    funcPattern.findFirstMatchIn(code) match {
      case Some(m) => m.group(1)
      case None    => "load"
    }
  }

  /**
    * Generates the full Python code with class wrapper.
    */
  private def generateFullCode(): String = {
    val funcName = parseFunctionName()

    s"""from pytexera import *
       |import pandas as pd
       |
       |class GenerateOperator(UDFSourceOperator):
       |    @overrides
       |    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
       |        yield $funcName()
       |
       |$code
       |""".stripMargin
  }

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val fullCode = generateFullCode()

    // Validate code for print statements (file I/O is allowed in source operators)
    val validatedCode =
      try {
        PythonCodeValidator.validateNoPrint(fullCode)
        fullCode
      } catch {
        case ex: Throwable =>
          PythonCodeValidator.generatePythonCodeForRaisingException(ex)
      }

    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithCode(validatedCode, "python")
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withIsOneToManyOp(true)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> Schema()))
      )
      .withLocationPreference(Option.empty)
      .withParallelizable(false)
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Data Loading",
      """Load data from files or external sources with a Python function. Do NOT use print statement
        |
        |Write a simple function - the operator handles the class boilerplate.
        |File I/O is allowed in this source operator.
        |
        |Example 1 - Load CSV:
        |  def load() -> pd.DataFrame:
        |      return pd.read_csv("/path/to/data.csv")
        |
        |Example 2 - Load JSON with processing:
        |  def load() -> pd.DataFrame:
        |      df = pd.read_json("/path/to/data.json")
        |      return df[df["status"] == "active"]
        |
        |Example 3 - Load text file as single row:
        |  def load() -> pd.DataFrame:
        |      with open("/path/to/readme.md", "r") as f:
        |          content = f.read()
        |      return pd.DataFrame([{"filename": "readme.md", "content": content}])
        |
        |""".stripMargin,
      OperatorGroupConstants.PYTHON_GROUP,
      List.empty, // No input ports for a source operator
      List(OutputPort(blocking = true)),
      supportReconfiguration = true
    )
  }

  override def sourceSchema(): Schema = Schema()
}
