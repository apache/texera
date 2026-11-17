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

import org.apache.texera.amber.core.tuple._
import org.apache.texera.amber.core.workflow.PortIdentity
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
    .add(
      new Attribute("field3", AttributeType.BOOLEAN),
      true
    )
    .build()
  val opDesc: ProjectionOpDesc = new ProjectionOpDesc()

  it should "open" in {
    opDesc.attributes = List(
      new AttributeUnit("field2", "f2"),
      new AttributeUnit("field1", "f1")
    )
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

  }

  it should "process Tuple" in {
    opDesc.attributes = List(
      new AttributeUnit("field2", "f2"),
      new AttributeUnit("field1", "f1")
    )
    val outputSchema = Schema()
      .add(new Attribute("f1", AttributeType.STRING))
      .add(new Attribute("f2", AttributeType.INTEGER))

    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

    val outputTuple =
      projectionOpExec
        .processTuple(tuple, 0)
        .next()
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
    assert(outputTuple.length == 2)
    assert(outputTuple.getField("f1").asInstanceOf[String] == "hello")
    assert(outputTuple.getField("f2").asInstanceOf[Int] == 1)
    assert(outputTuple.getField[String](0) == "hello")
    assert(outputTuple.getField[Int](1) == 1)
  }
  it should "process Tuple with different order" in {
    opDesc.attributes = List(
      new AttributeUnit("field3", "f3"),
      new AttributeUnit("field1", "f1")
    )
    val outputSchema = Schema()
      .add(new Attribute("f3", AttributeType.BOOLEAN))
      .add(new Attribute("f1", AttributeType.STRING))

    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

    val outputTuple =
      projectionOpExec
        .processTuple(tuple, 0)
        .next()
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
    assert(outputTuple.length == 2)
    assert(outputTuple.getField("f3").asInstanceOf[Boolean])
    assert(outputTuple.getField("f1").asInstanceOf[String] == "hello")
    assert(outputTuple.getField[Boolean](0))
    assert(outputTuple.getField[String](1) == "hello")
  }

  it should "meException on non-existing fields" in {
    opDesc.attributes = List(
      new AttributeUnit("field---5", "f5"),
      new AttributeUnit("field---6", "f6")
    )
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    assertThrows[RuntimeException] {
      projectionOpExec.processTuple(tuple, 0).next()
    }
  }

  it should "raise IllegalArgumentException on empty attributes" in {
    opDesc.attributes = List()
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    assertThrows[IllegalArgumentException] {
      projectionOpExec.processTuple(tuple, 0).next()
    }
  }

  it should "raise RuntimeException on duplicate alias" in {
    opDesc.attributes = List(
      new AttributeUnit("field1", "f"),
      new AttributeUnit("field2", "f")
    )
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    assertThrows[RuntimeException] {
      projectionOpExec.processTuple(tuple, 0).next()
    }
  }

  it should "allow empty alias" in {
    opDesc.attributes = List(
      new AttributeUnit("field2", "f2"),
      new AttributeUnit("field1", "")
    )
    val outputSchema = Schema()
      .add(new Attribute("field1", AttributeType.STRING))
      .add(new Attribute("f2", AttributeType.INTEGER))

    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

    val outputTuple =
      projectionOpExec
        .processTuple(tuple, 0)
        .next()
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)
    assert(outputTuple.length == 2)
    assert(outputTuple.getField("field1").asInstanceOf[String] == "hello")
    assert(outputTuple.getField("f2").asInstanceOf[Int] == 1)
    assert(outputTuple.getField[String](0) == "hello")
    assert(outputTuple.getField[Int](1) == 1)
  }

  it should "drop a single attribute and keep the rest under their original names" in {
    opDesc.isDrop = true
    opDesc.attributes = List(
      new AttributeUnit("field2", "")
    )
    val outputSchema = Schema()
      .add(new Attribute("field1", AttributeType.STRING))
      .add(new Attribute("field3", AttributeType.BOOLEAN))

    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

    val output = projectionOpExec.processTuple(tuple, 0).next().asInstanceOf[MapTupleLike]
    assert(output.fieldMappings.keySet == Set("field1", "field3"))

    val outputTuple = output.enforceSchema(outputSchema)
    assert(outputTuple.length == 2)
    assert(outputTuple.getField[String](0) == "hello")
    assert(outputTuple.getField[Boolean](1))
  }

  it should "drop multiple attributes" in {
    opDesc.isDrop = true
    opDesc.attributes = List(
      new AttributeUnit("field1", ""),
      new AttributeUnit("field3", "")
    )
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

    val output = projectionOpExec.processTuple(tuple, 0).next().asInstanceOf[MapTupleLike]
    assert(output.fieldMappings == Map("field2" -> 1))
  }

  it should "ignore aliases in drop mode" in {
    opDesc.isDrop = true
    opDesc.attributes = List(
      new AttributeUnit("field2", "renamed")
    )
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

    val output = projectionOpExec.processTuple(tuple, 0).next().asInstanceOf[MapTupleLike]
    assert(output.fieldMappings.keySet == Set("field1", "field3"))
    assert(!output.fieldMappings.contains("renamed"))
  }

  it should "silently ignore dropping a non-existent attribute" in {
    opDesc.isDrop = true
    opDesc.attributes = List(
      new AttributeUnit("field---5", "f5")
    )
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

    val output = projectionOpExec.processTuple(tuple, 0).next().asInstanceOf[MapTupleLike]
    assert(output.fieldMappings == Map("field1" -> "hello", "field2" -> 1, "field3" -> true))
  }

  it should "emit an empty tuple when dropping every attribute" in {
    opDesc.isDrop = true
    opDesc.attributes = List(
      new AttributeUnit("field1", ""),
      new AttributeUnit("field2", ""),
      new AttributeUnit("field3", "")
    )
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

    val output = projectionOpExec.processTuple(tuple, 0).next().asInstanceOf[MapTupleLike]
    assert(output.fieldMappings.isEmpty)
  }

  it should "emit exactly the attributes the descriptor derives for the same drop config" in {
    opDesc.isDrop = true
    opDesc.attributes = List(
      new AttributeUnit("field2", "")
    )
    val derivedSchema =
      opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head

    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

    val output = projectionOpExec.processTuple(tuple, 0).next().asInstanceOf[MapTupleLike]
    assert(output.fieldMappings.keySet == derivedSchema.getAttributeNames.toSet)

    val outputTuple = output.enforceSchema(derivedSchema)
    assert(outputTuple.length == 2)
    assert(outputTuple.getField[String]("field1") == "hello")
    assert(outputTuple.getField[Boolean]("field3"))
  }

  it should "match drop names case-insensitively" in {
    // Matches the descriptor, whose Schema.remove lowercases both sides.
    opDesc.isDrop = true
    opDesc.attributes = List(
      new AttributeUnit("FIELD2", "")
    )
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

    val output = projectionOpExec.processTuple(tuple, 0).next().asInstanceOf[MapTupleLike]
    assert(output.fieldMappings == Map("field1" -> "hello", "field3" -> true))
  }

  it should "tolerate duplicate entries in the drop list" in {
    opDesc.isDrop = true
    opDesc.attributes = List(
      new AttributeUnit("field2", ""),
      new AttributeUnit("field2", "")
    )
    val projectionOpExec = new ProjectionOpExec(objectMapper.writeValueAsString(opDesc))
    projectionOpExec.open()

    val output = projectionOpExec.processTuple(tuple, 0).next().asInstanceOf[MapTupleLike]
    assert(output.fieldMappings == Map("field1" -> "hello", "field3" -> true))
  }
}
