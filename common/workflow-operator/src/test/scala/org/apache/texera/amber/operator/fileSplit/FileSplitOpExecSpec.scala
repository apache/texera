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

package org.apache.texera.amber.operator.fileSplit

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class FileSplitOpExecSpec extends AnyFlatSpec {

  "FileSplitOpExec" should "keep rows from the same file on the same output port" in {
    val desc = new FileSplitOpDesc()
    val exec = new FileSplitOpExec(objectMapper.writeValueAsString(desc))
    val schema = Schema(
      List(
        new Attribute("source_file", AttributeType.STRING),
        new Attribute("value", AttributeType.INTEGER)
      )
    )

    exec.open()
    val outputs = List(
      Tuple(schema, Array[Any]("a.csv", 1)),
      Tuple(schema, Array[Any]("b.csv", 2)),
      Tuple(schema, Array[Any]("a.csv", 3)),
      Tuple(schema, Array[Any]("c.csv", 4))
    ).flatMap(tuple => exec.processTupleMultiPort(tuple, 0).toList)
    exec.close()

    assert(outputs.map(_._2.get) == List(PortIdentity(), PortIdentity(1), PortIdentity(), PortIdentity()))
  }

  it should "auto-detect the filename column used by file scans" in {
    val desc = new FileSplitOpDesc()
    val exec = new FileSplitOpExec(objectMapper.writeValueAsString(desc))
    val schema = Schema(
      List(
        new Attribute("filename", AttributeType.STRING),
        new Attribute("content", AttributeType.BINARY)
      )
    )

    exec.open()
    val output = exec
      .processTupleMultiPort(Tuple(schema, Array[Any]("cat.png", Array[Byte](1, 2, 3))), 0)
      .next()
    exec.close()

    assert(output._2.contains(PortIdentity()))
  }
}
