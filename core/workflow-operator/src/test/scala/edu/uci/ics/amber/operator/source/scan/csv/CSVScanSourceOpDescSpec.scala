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

package edu.uci.ics.amber.operator.source.scan.csv

import edu.uci.ics.amber.core.storage.FileResolver
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.WorkflowContext.{DEFAULT_EXECUTION_ID, DEFAULT_WORKFLOW_ID}
import edu.uci.ics.amber.operator.TestOperators
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class CSVScanSourceOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var csvScanSourceOpDesc: CSVScanSourceOpDesc = _
  var parallelCsvScanSourceOpDesc: ParallelCSVScanSourceOpDesc = _
  before {
    csvScanSourceOpDesc = new CSVScanSourceOpDesc()
    parallelCsvScanSourceOpDesc = new ParallelCSVScanSourceOpDesc()
  }

  it should "infer schema from single-line-data csv" in {

    parallelCsvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesSmallCsvPath)
    parallelCsvScanSourceOpDesc.customDelimiter = Some(",")
    parallelCsvScanSourceOpDesc.hasHeader = true
    parallelCsvScanSourceOpDesc.setResolvedFileName(
      FileResolver.resolve(parallelCsvScanSourceOpDesc.fileName.get)
    )
    val inferredSchema: Schema = parallelCsvScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 14)
    assert(inferredSchema.getAttribute("Order ID").getType == AttributeType.INTEGER)
    assert(inferredSchema.getAttribute("Unit Price").getType == AttributeType.DOUBLE)

  }

  it should "infer schema from headerless single-line-data csv" in {

    parallelCsvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesHeaderlessSmallCsvPath)
    parallelCsvScanSourceOpDesc.customDelimiter = Some(",")
    parallelCsvScanSourceOpDesc.hasHeader = false
    parallelCsvScanSourceOpDesc.setResolvedFileName(
      FileResolver.resolve(parallelCsvScanSourceOpDesc.fileName.get)
    )

    val inferredSchema: Schema = parallelCsvScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 14)
    assert(inferredSchema.getAttribute("column-10").getType == AttributeType.DOUBLE)
    assert(inferredSchema.getAttribute("column-7").getType == AttributeType.INTEGER)
  }

  it should "infer schema from multi-line-data csv" in {

    csvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesSmallMultiLineCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.hasHeader = true
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(csvScanSourceOpDesc.fileName.get))

    val inferredSchema: Schema = csvScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 14)
    assert(inferredSchema.getAttribute("Order ID").getType == AttributeType.INTEGER)
    assert(inferredSchema.getAttribute("Unit Price").getType == AttributeType.DOUBLE)
  }

  it should "infer schema from headerless multi-line-data csv" in {

    csvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesHeaderlessSmallCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.hasHeader = false
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(csvScanSourceOpDesc.fileName.get))

    val inferredSchema: Schema = csvScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 14)
    assert(inferredSchema.getAttribute("column-10").getType == AttributeType.DOUBLE)
    assert(inferredSchema.getAttribute("column-7").getType == AttributeType.INTEGER)
  }

  it should "infer schema from headerless multi-line-data csv with custom delimiter" in {

    csvScanSourceOpDesc.fileName =
      Some(TestOperators.CountrySalesSmallMultiLineCustomDelimiterCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(";")
    csvScanSourceOpDesc.hasHeader = false
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(csvScanSourceOpDesc.fileName.get))

    val inferredSchema: Schema = csvScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 14)
    assert(inferredSchema.getAttribute("column-10").getType == AttributeType.DOUBLE)
    assert(inferredSchema.getAttribute("column-7").getType == AttributeType.INTEGER)
  }

  it should "create one worker with multi-line-data csv" in {

    csvScanSourceOpDesc.fileName =
      Some(TestOperators.CountrySalesSmallMultiLineCustomDelimiterCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(";")
    csvScanSourceOpDesc.hasHeader = false
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(csvScanSourceOpDesc.fileName.get))

    assert(
      !csvScanSourceOpDesc
        .getPhysicalOp(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)
        .parallelizable
    )
  }

}
