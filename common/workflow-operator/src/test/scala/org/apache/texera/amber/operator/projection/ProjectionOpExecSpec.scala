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

package org.apache.texera.amber.operator.projection

import org.apache.texera.amber.core.executor.ColumnarResult
import org.apache.texera.amber.core.tuple._
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class ProjectionOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))

  val tuple: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "hello")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(new Attribute("field3", AttributeType.BOOLEAN), true)
    .build()
  val opDesc: ProjectionOpDesc = new ProjectionOpDesc()

  before {
    opDesc.isDrop = false
    opDesc.attributes = List()
  }

  private def units(pairs: (String, String)*): List[AttributeUnit] =
    pairs.map { case (orig, alias) => new AttributeUnit(orig, alias) }.toList

  private def execProjection(isDrop: Boolean = false, openExec: Boolean = true)(
      pairs: (String, String)*
  ): TupleLike = {
    opDesc.isDrop = isDrop
    opDesc.attributes = units(pairs: _*)
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    if (openExec) projectionOpExec.open()
    projectionOpExec.processTuple(tuple, 0).next()
  }

  private def enforce(outputSchema: Schema)(pairs: (String, String)*): Tuple =
    execProjection()(pairs: _*).asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema)

  private def dropOutput(pairs: (String, String)*): MapTupleLike =
    execProjection(isDrop = true)(pairs: _*).asInstanceOf[MapTupleLike]

  it should "open" in {
    opDesc.attributes = units("field2" -> "f2", "field1" -> "f1")
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()
  }

  it should "process Tuple" in {
    val outputSchema = Schema()
      .add(new Attribute("f1", AttributeType.STRING))
      .add(new Attribute("f2", AttributeType.INTEGER))
    val outputTuple = enforce(outputSchema)("field2" -> "f2", "field1" -> "f1")
    assert(outputTuple.length == 2)
    assert(outputTuple.getField("f1").asInstanceOf[String] == "hello")
    assert(outputTuple.getField("f2").asInstanceOf[Int] == 1)
    assert(outputTuple.getField[String](0) == "hello")
    assert(outputTuple.getField[Int](1) == 1)
  }

  it should "process Tuple with different order" in {
    val outputSchema = Schema()
      .add(new Attribute("f3", AttributeType.BOOLEAN))
      .add(new Attribute("f1", AttributeType.STRING))
    val outputTuple = enforce(outputSchema)("field3" -> "f3", "field1" -> "f1")
    assert(outputTuple.length == 2)
    assert(outputTuple.getField("f3").asInstanceOf[Boolean])
    assert(outputTuple.getField("f1").asInstanceOf[String] == "hello")
    assert(outputTuple.getField[Boolean](0))
    assert(outputTuple.getField[String](1) == "hello")
  }

  it should "meException on non-existing fields" in {
    assertThrows[RuntimeException] {
      execProjection(openExec = false)("field---5" -> "f5", "field---6" -> "f6")
    }
  }

  it should "raise IllegalArgumentException on empty attributes" in {
    assertThrows[IllegalArgumentException] {
      execProjection(openExec = false)()
    }
  }

  it should "raise RuntimeException on duplicate alias" in {
    assertThrows[RuntimeException] {
      execProjection(openExec = false)("field1" -> "f", "field2" -> "f")
    }
  }

  it should "allow empty alias" in {
    val outputSchema = Schema()
      .add(new Attribute("field1", AttributeType.STRING))
      .add(new Attribute("f2", AttributeType.INTEGER))
    val outputTuple = enforce(outputSchema)("field2" -> "f2", "field1" -> "")
    assert(outputTuple.length == 2)
    assert(outputTuple.getField("field1").asInstanceOf[String] == "hello")
    assert(outputTuple.getField("f2").asInstanceOf[Int] == 1)
    assert(outputTuple.getField[String](0) == "hello")
    assert(outputTuple.getField[Int](1) == 1)
  }

  it should "drop a single attribute and keep the rest under their original names" in {
    val outputSchema = Schema()
      .add(new Attribute("field1", AttributeType.STRING))
      .add(new Attribute("field3", AttributeType.BOOLEAN))
    val output = dropOutput("field2" -> "")
    assert(output.fieldMappings.keySet == Set("field1", "field3"))

    val outputTuple = output.enforceSchema(outputSchema)
    assert(outputTuple.length == 2)
    assert(outputTuple.getField[String](0) == "hello")
    assert(outputTuple.getField[Boolean](1))
  }

  it should "drop multiple attributes" in {
    val output = dropOutput("field1" -> "", "field3" -> "")
    assert(output.fieldMappings == Map("field2" -> 1))
  }

  it should "ignore aliases in drop mode" in {
    val output = dropOutput("field2" -> "renamed")
    assert(output.fieldMappings.keySet == Set("field1", "field3"))
    assert(!output.fieldMappings.contains("renamed"))
  }

  it should "silently ignore dropping a non-existent attribute" in {
    val output = dropOutput("field---5" -> "f5")
    assert(output.fieldMappings == Map("field1" -> "hello", "field2" -> 1, "field3" -> true))
  }

  it should "emit an empty tuple when dropping every attribute" in {
    val output = dropOutput("field1" -> "", "field2" -> "", "field3" -> "")
    assert(output.fieldMappings.isEmpty)
  }

  it should "emit exactly the attributes the descriptor derives for the same drop config" in {
    opDesc.isDrop = true
    opDesc.attributes = units("field2" -> "")
    val derivedSchema =
      opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
    val output = dropOutput("field2" -> "")
    assert(output.fieldMappings.keySet == derivedSchema.getAttributeNames.toSet)

    val outputTuple = output.enforceSchema(derivedSchema)
    assert(outputTuple.length == 2)
    assert(outputTuple.getField[String]("field1") == "hello")
    assert(outputTuple.getField[Boolean]("field3"))
  }

  it should "match drop names case-insensitively" in {
    // Matches the descriptor, whose Schema.remove lowercases both sides.
    val output = dropOutput("FIELD2" -> "")
    assert(output.fieldMappings == Map("field1" -> "hello", "field3" -> true))
  }

  it should "tolerate duplicate entries in the drop list" in {
    val output = dropOutput("field2" -> "", "field2" -> "")
    assert(output.fieldMappings == Map("field1" -> "hello", "field3" -> true))
  }

  // ---- Native-Arrow path (columnar projection) ----

  private val tuple2: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "world")
    .add(new Attribute("field2", AttributeType.INTEGER), 2)
    .add(new Attribute("field3", AttributeType.BOOLEAN), false)
    .build()

  // Run the batch through processColumnarBatch and decode the emitted Arrow result.
  private def columnarProject(exec: ProjectionOpExec, rows: Array[Tuple]): Array[Tuple] =
    exec.processColumnarBatch(ArrowUtils.serializeTuples(tupleSchema, rows), 0) match {
      case ColumnarResult.Emit(root) =>
        try ArrowUtils.tuplesOfRoot(root)
        finally root.close()
      case other => fail(s"expected Emit, got: $other")
    }

  it should "project and rename columns on an Arrow batch identically to the row path" in {
    opDesc.isDrop = false
    opDesc.attributes = List(new AttributeUnit("field2", "f2"), new AttributeUnit("field1", "f1"))
    val outputSchema = Schema()
      .add(new Attribute("f2", AttributeType.INTEGER))
      .add(new Attribute("f1", AttributeType.STRING))
    val exec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    exec.open()

    val rows = Array(tuple, tuple2)
    val rowOut = rows.map(
      exec.processTuple(_, 0).next().asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema)
    )
    val colOut = columnarProject(exec, rows)

    assert(colOut.length == rows.length)
    colOut.zip(rowOut).foreach {
      case (c, r) =>
        assert(c.getSchema == outputSchema)
        assert(c.getField[Any]("f1") == r.getField[Any]("f1"))
        assert(c.getField[Any]("f2") == r.getField[Any]("f2"))
    }
    exec.close()
  }

  it should "drop columns on an Arrow batch identically to the row path" in {
    opDesc.isDrop = true
    opDesc.attributes = List(new AttributeUnit("field2", ""))
    val outputSchema = Schema()
      .add(new Attribute("field1", AttributeType.STRING))
      .add(new Attribute("field3", AttributeType.BOOLEAN))
    val exec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    exec.open()

    val colOut = columnarProject(exec, Array(tuple, tuple2))
    assert(colOut.length == 2)
    assert(colOut.head.getSchema == outputSchema)
    assert(colOut.head.getField[String]("field1") == "hello")
    assert(colOut.head.getField[Boolean]("field3"))
    assert(colOut(1).getField[String]("field1") == "world")
    assert(!colOut(1).getField[Boolean]("field3"))
    exec.close()
  }

  it should "project a live root (zero-copy) identically to processColumnarBatch" in {
    opDesc.isDrop = false
    opDesc.attributes = List(new AttributeUnit("field2", "f2"), new AttributeUnit("field1", "f1"))
    val exec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    exec.open()
    val rows = Array(tuple, tuple2)
    val viaBytes = columnarProject(exec, rows)
    val alloc = new org.apache.arrow.memory.RootAllocator()
    val viaRoot =
      try {
        ArrowUtils.deserializeRootFold(ArrowUtils.serializeTuples(tupleSchema, rows), alloc) {
          root =>
            exec.processColumnarRoot(root, 0) match {
              case ColumnarResult.Emit(out) =>
                try ArrowUtils.tuplesOfRoot(out)
                finally out.close()
              case other => fail(s"expected Emit, got: $other")
            }
        }
      } finally alloc.close()
    assert(viaRoot.length == viaBytes.length)
    viaRoot.zip(viaBytes).foreach {
      case (r, b) =>
        assert(r.getSchema == b.getSchema)
        assert(r.getField[Any]("f1") == b.getField[Any]("f1"))
        assert(r.getField[Any]("f2") == b.getField[Any]("f2"))
    }
    exec.close()
  }

  it should "fall back to the row path (Unsupported) on a duplicate alias" in {
    opDesc.isDrop = false
    opDesc.attributes = List(new AttributeUnit("field1", "f"), new AttributeUnit("field2", "f"))
    val exec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    exec.open()
    assert(
      exec.processColumnarBatch(ArrowUtils.serializeTuples(tupleSchema, Array(tuple)), 0) ==
        ColumnarResult.Unsupported
    )
    exec.close()
  }
}
