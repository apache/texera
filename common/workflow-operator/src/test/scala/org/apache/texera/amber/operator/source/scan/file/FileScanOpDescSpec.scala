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

package org.apache.texera.amber.operator.source.scan.file

import org.apache.texera.amber.core.tuple.{
  Attribute,
  AttributeType,
  Schema,
  SchemaEnforceable,
  Tuple
}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.source.scan.{FileAttributeType, FileDecodingMethod}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class FileScanOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  private val inputSchema = new Schema(new Attribute("filename", AttributeType.STRING))

  var fileScanOpDesc: FileScanOpDesc = _

  before {
    fileScanOpDesc = new FileScanOpDesc()
    fileScanOpDesc.fileEncoding = FileDecodingMethod.UTF_8
  }

  it should "infer schema with single column representing each line of text" in {
    val inferredSchema: Schema = fileScanOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 1)
    assert(inferredSchema.getAttribute("line").getType == AttributeType.STRING)
  }

  it should "read first 5 lines from the input file path tuple into output tuples" in {
    fileScanOpDesc.attributeType = FileAttributeType.STRING
    fileScanOpDesc.fileScanLimit = Option(5)

    val inputTuple = Tuple(inputSchema, Array[Any](TestOperators.TestTextFilePath))
    val fileScanOpExec =
      new FileScanOpExec(objectMapper.writeValueAsString(fileScanOpDesc))

    fileScanOpExec.open()
    val processedTuple: Iterator[Tuple] = fileScanOpExec
      .processTuple(inputTuple, 0)
      .map(tupleLike =>
        tupleLike
          .asInstanceOf[SchemaEnforceable]
          .enforceSchema(fileScanOpDesc.sourceSchema())
      )

    assert(processedTuple.next().getField("line").equals("line1"))
    assert(processedTuple.next().getField("line").equals("line2"))
    assert(processedTuple.next().getField("line").equals("line3"))
    assert(processedTuple.next().getField("line").equals("line4"))
    assert(processedTuple.next().getField("line").equals("line5"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("line"))
    fileScanOpExec.close()
  }

  it should "preserve the original input filename when include filename is enabled" in {
    fileScanOpDesc.attributeType = FileAttributeType.SINGLE_STRING
    fileScanOpDesc.outputFileName = true

    val inputFilePath = TestOperators.TestTextFilePath
    val inputTuple = Tuple(inputSchema, Array[Any](inputFilePath))
    val fileScanOpExec =
      new FileScanOpExec(objectMapper.writeValueAsString(fileScanOpDesc))

    fileScanOpExec.open()
    val outputSchema = fileScanOpDesc.sourceSchema()
    val processedTuple = fileScanOpExec
      .processTuple(inputTuple, 0)
      .next()
      .asInstanceOf[SchemaEnforceable]
      .enforceSchema(outputSchema)

    assert(processedTuple.getField[String]("filename") == inputFilePath)
    fileScanOpExec.close()
  }
}
