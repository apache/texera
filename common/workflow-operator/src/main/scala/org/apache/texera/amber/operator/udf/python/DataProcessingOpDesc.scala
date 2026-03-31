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

package org.apache.texera.amber.operator.udf.python

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.{
  LogicalOp,
  PortDescription,
  PythonCodeValidator,
  StateTransferFunc
}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

import scala.util.{Success, Try}
import scala.util.matching.Regex

/**
  * DataProcessingOp operator for processing multiple input tables with a simple function.
  *
  * This operator allows users to write a simple Python function that takes input tables
  * as parameters. The operator automatically parses the function signature to determine
  * input ports and generates the full class code.
  *
  * Example user code:
  * {{{
  * def process(orders, customers) -> pd.DataFrame:
  *     merged = orders.merge(customers, on='customer_id')
  *     return merged[['order_id', 'customer_name', 'total']]
  * }}}
  *
  * Generated code:
  * {{{
  * from pytexera import *
  *
  * class ProcessTablesOperator(UDFMultiTableOperator):
  *     INPUT_PORTS = ["orders", "customers"]
  *
  *     def process_tables(self) -> Iterator[Optional[TableLike]]:
  *         yield process(self.orders, self.customers)
  *
  * def process(orders, customers) -> pd.DataFrame:
  *     merged = orders.merge(customers, on='customer_id')
  *     return merged[['order_id', 'customer_name', 'total']]
  * }}}
  */
class DataProcessingOpDesc extends LogicalOp {

  @JsonProperty(
    required = true,
    defaultValue =
      "def process(input_0) -> pd.DataFrame:\n" +
        "    # Process the input table(s) and return a DataFrame\n" +
        "    # Parameter names become input port names\n" +
        "    # NEVER do any file IO\n" +
        "    return input_0\n"
  )
  @JsonSchemaTitle("Python function")
  @JsonPropertyDescription("input your code here")
  var code: String = ""

  /**
    * Parses the function definition to extract parameter names.
    * Supports: def process(table1, table2) or def process(table1, table2) -> pd.DataFrame:
    */
  private def parseParameters(): List[String] = {
    val funcPattern: Regex = """def\s+\w+\s*\(([^)]*)\)""".r
    funcPattern.findFirstMatchIn(code) match {
      case Some(m) =>
        val paramsStr = m.group(1).trim
        if (paramsStr.isEmpty) {
          List.empty
        } else {
          paramsStr
            .split(",")
            .map(_.trim)
            .map { param =>
              // Handle type annotations like "table1: pd.DataFrame"
              param.split(":")(0).trim
            }
            .filter(_.nonEmpty)
            .toList
        }
      case None => List("input_0") // Default if no valid function found
    }
  }

  /**
    * Generates the full Python code with class wrapper.
    */
  private def generateFullCode(): String = {
    val params = parseParameters()
    val inputPorts = params.map(p => s""""$p"""").mkString(", ")
    val selfParams = params.map(p => s"self.$p").mkString(", ")

    s"""from pytexera import *
       |import pandas as pd
       |
       |class ProcessTablesOperator(UDFMultiTableOperator):
       |    INPUT_PORTS = [$inputPorts]
       |
       |    def process_tables(self) -> Iterator[Optional[TableLike]]:
       |        yield process($selfParams)
       |
       |$code
       |""".stripMargin
  }

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val opInfo = this.operatorInfo
    val partitionRequirement: List[Option[PartitionInfo]] = if (inputPorts != null) {
      inputPorts.map(p => Option(p.partitionRequirement))
    } else {
      opInfo.inputPorts.map(_ => None)
    }

    val fullCode = generateFullCode()

    // Validate code for file I/O operations and print statements
    val validatedCode =
      try {
        PythonCodeValidator.validateNoFileIO(fullCode)
        PythonCodeValidator.validateNoPrint(fullCode)
        fullCode
      } catch {
        case ex: Throwable =>
          PythonCodeValidator.generatePythonCodeForRaisingException(ex)
      }

    // Schema is always empty - runtime inference handles actual schema
    val propagateSchema = (_: Map[PortIdentity, Schema]) => {
      Map(operatorInfo.outputPorts.head.id -> Schema())
    }

    val physicalOp = PhysicalOp
      .manyToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithCode(validatedCode, "python")
      )
      .withParallelizable(false)

    physicalOp
      .withDerivePartition(_ => UnknownPartition())
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(partitionRequirement)
      .withIsOneToManyOp(true)
      .withPropagateSchema(SchemaPropagationFunc(propagateSchema))
  }

  override def operatorInfo: OperatorInfo = {
    val params = parseParameters()

    val inputPortInfo = if (inputPorts != null && inputPorts.nonEmpty) {
      inputPorts.zipWithIndex.map {
        case (portDesc: PortDescription, idx) =>
          // Port dependencies ensure data is processed in order (port 0 before port 1, etc.)
          val previousPortDependencies = (0 until idx).map(PortIdentity(_)).toList
          InputPort(
            PortIdentity(idx),
            displayName = portDesc.displayName,
            allowMultiLinks = portDesc.allowMultiInputs,
            dependencies = previousPortDependencies
          )
      }
    } else {
      // Create input ports based on parsed parameters
      params.zipWithIndex.map {
        case (paramName, idx) =>
          // Port dependencies ensure data is processed in order
          val previousPortDependencies = (0 until idx).map(PortIdentity(_)).toList
          InputPort(
            PortIdentity(idx),
            displayName = paramName,
            allowMultiLinks = true,
            dependencies = previousPortDependencies
          )
      }
    }

    val outputPortInfo = if (outputPorts != null) {
      outputPorts.zipWithIndex.map {
        case (portDesc, idx) =>
          OutputPort(PortIdentity(idx), displayName = portDesc.displayName, blocking = true)
      }
    } else {
      List(OutputPort(blocking = true))
    }

    OperatorInfo(
      "Data Processing",
      """Process input tables from input ports with a Python function. Do NOT use print statement
        |
        |Function parameters correspond to input ports in order: the 1st parameter receives data from input port 0, the 2nd from input port 1, and so on.
        |
        |Example 1 - Filter and transform:
        |  def process(users) -> pd.DataFrame:
        |      filtered = users[users["age"] > 18]
        |      return filtered[["name", "age", "city"]]
        |
        |Example 2 - Join two tables:
        |  def process(orders, customers) -> pd.DataFrame:
        |      merged = orders.merge(customers, on="customer_id")
        |      return merged[["order_id", "customer_name", "total"]]
        |
        |Example 3 - Aggregate data:
        |  def process(sales) -> pd.DataFrame:
        |      return sales.groupby("product").agg({"amount": "sum"}).reset_index()
        |""".stripMargin,
      OperatorGroupConstants.PYTHON_GROUP,
      inputPortInfo,
      outputPortInfo,
      dynamicInputPorts = true,
      dynamicOutputPorts = true,
      supportReconfiguration = true,
      allowPortCustomization = true
    )
  }

  override def runtimeReconfiguration(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      oldLogicalOp: LogicalOp,
      newLogicalOp: LogicalOp
  ): Try[(PhysicalOp, Option[StateTransferFunc])] = {
    Success(newLogicalOp.getPhysicalOp(workflowId, executionId), None)
  }
}
