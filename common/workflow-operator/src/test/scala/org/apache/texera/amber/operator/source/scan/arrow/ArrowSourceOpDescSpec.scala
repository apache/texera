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

package org.apache.texera.amber.operator.source.scan.arrow

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.source.scan.FileDecodingMethod
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileOutputStream}
import java.nio.channels.Channels
import java.nio.file.Files

class ArrowSourceOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def writeArrowFile(schema: Schema, rows: Seq[Array[Any]]): File = {
    val file = File.createTempFile("arrow-src-", ".arrow")
    file.deleteOnExit()
    val allocator = new RootAllocator()
    val root = VectorSchemaRoot.create(ArrowUtils.fromTexeraSchema(schema), allocator)
    val out = new FileOutputStream(file)
    val writer = new ArrowFileWriter(root, null, Channels.newChannel(out))
    try {
      writer.start()
      root.allocateNew()
      rows.zipWithIndex.foreach {
        case (values, i) =>
          ArrowUtils.setTexeraTuple(Tuple.builder(schema).addSequentially(values).build(), i, root)
      }
      root.setRowCount(rows.size)
      writer.writeBatch()
      writer.end()
    } finally {
      writer.close()
      root.close()
      allocator.close()
      out.close()
    }
    file
  }

  "ArrowSourceOpDesc.operatorInfo" should
    "advertise the Arrow file-scan name in the Data Input group with no input and one output" in {
    val info = (new ArrowSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Arrow File Scan"
    info.operatorDescription shouldBe "Scan data from an Arrow file"
    info.operatorGroupName shouldBe OperatorGroupConstants.INPUT_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "ArrowSourceOpDesc" should "default the encoding and scan window" in {
    val d = new ArrowSourceOpDesc
    d.fileName shouldBe None
    d.fileEncoding shouldBe FileDecodingMethod.UTF_8
    d.limit shouldBe None
    d.offset shouldBe None
    d.fileTypeName shouldBe Some("Arrow")
  }

  "ArrowSourceOpDesc.sourceSchema" should "be null before a file is resolved" in {
    (new ArrowSourceOpDesc).sourceSchema() shouldBe null
  }

  "ArrowSourceOpDesc.getPhysicalOp" should
    "wire the Arrow exec as a source op with no input port and one output port" in {
    val d = new ArrowSourceOpDesc
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.source.scan.arrow.ArrowSourceOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe empty
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  "ArrowSourceOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new ArrowSourceOpDesc
    d.fileName = Some("file:///tmp/data.arrow")
    d.limit = Some(7)
    d.offset = Some(3)
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[ArrowSourceOpDesc]
    val r = restored.asInstanceOf[ArrowSourceOpDesc]
    r.fileName shouldBe Some("file:///tmp/data.arrow")
    r.limit shouldBe Some(7)
    r.offset shouldBe Some(3)
  }

  "ArrowSourceOpDesc.inferSchema" should "infer the Texera schema from a valid Arrow file" in {
    val schema = Schema(List(new Attribute("s", AttributeType.STRING)))
    val file = writeArrowFile(schema, Seq(Array[Any]("a"), Array[Any]("b")))
    val d = new ArrowSourceOpDesc
    d.fileName = Some(file.toURI.toString)
    val inferred = d.inferSchema()
    inferred.getAttributes should have length 1
    inferred.getAttributes.head.getName shouldBe "s"
    inferred.getAttributes.head.getType shouldBe AttributeType.STRING
  }

  it should "infer every supported attribute type from a file containing null values" in {
    // Every AttributeType round-trips through Arrow (LARGE_BINARY/ANY are tagged in field
    // metadata). A single all-null row exercises the null-writing path for each type while
    // still producing a file whose schema spans all supported types. Exhaustive value/null
    // round-tripping itself is covered by ArrowUtilsSpec.
    val schema = Schema(
      List(
        new Attribute("i", AttributeType.INTEGER),
        new Attribute("l", AttributeType.LONG),
        new Attribute("d", AttributeType.DOUBLE),
        new Attribute("b", AttributeType.BOOLEAN),
        new Attribute("s", AttributeType.STRING),
        new Attribute("t", AttributeType.TIMESTAMP),
        new Attribute("bin", AttributeType.BINARY),
        new Attribute("lbin", AttributeType.LARGE_BINARY),
        new Attribute("any", AttributeType.ANY)
      )
    )
    val nullRow = Array.fill[Any](schema.getAttributes.length)(null)
    val file = writeArrowFile(schema, Seq(nullRow))
    val d = new ArrowSourceOpDesc
    d.fileName = Some(file.toURI.toString)
    d.inferSchema() shouldBe schema
  }

  it should "throw an IOException when the file is not a valid Arrow file" in {
    val bogus = File.createTempFile("not-arrow-", ".arrow")
    bogus.deleteOnExit()
    Files.write(bogus.toPath, "this is not arrow".getBytes)
    val d = new ArrowSourceOpDesc
    d.fileName = Some(bogus.toURI.toString)
    val ex = intercept[java.io.IOException](d.inferSchema())
    ex.getMessage shouldBe "Failed to infer schema from Arrow file."
  }
}
