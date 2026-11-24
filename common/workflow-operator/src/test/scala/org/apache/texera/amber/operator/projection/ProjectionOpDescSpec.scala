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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.workflow.{
  HashPartition,
  PortIdentity,
  RangePartition,
  SinglePartition,
  UnknownPartition
}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class ProjectionOpDescSpec extends AnyFlatSpec with BeforeAndAfter {
  val schema = new Schema(
    new Attribute("field1", AttributeType.STRING),
    new Attribute("field2", AttributeType.INTEGER),
    new Attribute("field3", AttributeType.BOOLEAN)
  )
  var projectionOpDesc: ProjectionOpDesc = _

  before {
    projectionOpDesc = new ProjectionOpDesc()
  }

  private def units(pairs: (String, String)*): List[AttributeUnit] =
    pairs.map { case (orig, alias) => new AttributeUnit(orig, alias) }.toList

  private def deriveSchema(isDrop: Boolean = false)(pairs: (String, String)*): Schema = {
    projectionOpDesc.isDrop = isDrop
    projectionOpDesc.attributes ++= units(pairs: _*)
    projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
  }

  it should "take in attribute names" in {
    projectionOpDesc.attributes ++= units("field1" -> "f1", "fields2" -> "f2")
    assert(projectionOpDesc.attributes.length == 2)
  }

  it should "filter schema correctly" in {
    val outputSchema = deriveSchema()("field1" -> "f1", "field2" -> "f2")
    assert(outputSchema.getAttributes.length == 2)
  }

  it should "reorder schema" in {
    val outputSchema = deriveSchema()("field2" -> "f2", "field1" -> "f1")
    assert(outputSchema.getAttributes.length == 2)
    assert(outputSchema.getIndex("f2") == 0)
    assert(outputSchema.getIndex("f1") == 1)
  }

  it should "raise RuntimeException on non-existing fields" in {
    assertThrows[RuntimeException] {
      deriveSchema()("field---5" -> "f5", "field---6" -> "f6")
    }
  }

  it should "raise IllegalArgumentException on empty attributes" in {
    assertThrows[IllegalArgumentException] {
      deriveSchema()()
    }
  }

  it should "raise RuntimeException on duplicate alias" in {
    assertThrows[RuntimeException] {
      deriveSchema()("field2" -> "f", "field1" -> "f")
    }
  }

  it should "allow alias to be optional" in {
    val outputSchema = deriveSchema()("field1" -> "f1", "field2" -> "")
    assert(outputSchema.getAttributes.length == 2)
  }

  it should "derive the drop-mode schema with original names, types and order" in {
    val outputSchema = deriveSchema(isDrop = true)("field2" -> "")
    assert(outputSchema.getAttributes.length == 2)
    assert(outputSchema.getIndex("field1") == 0)
    assert(outputSchema.getIndex("field3") == 1)
    assert(outputSchema.getAttribute("field1").getType == AttributeType.STRING)
    assert(outputSchema.getAttribute("field3").getType == AttributeType.BOOLEAN)
  }

  it should "ignore aliases when deriving the drop-mode schema" in {
    val outputSchema = deriveSchema(isDrop = true)("field1" -> "renamed")
    assert(outputSchema.getAttributeNames == List("field2", "field3"))
  }

  it should "raise IllegalArgumentException when dropping a non-existent attribute" in {
    // Unlike the exec, which silently ignores unknown names,
    // Schema.remove rejects them at schema-derivation time.
    assertThrows[IllegalArgumentException] {
      deriveSchema(isDrop = true)("field---5" -> "f5")
    }
  }

  it should "derive an empty schema when dropping every attribute" in {
    val outputSchema =
      deriveSchema(isDrop = true)("field1" -> "", "field2" -> "", "field3" -> "")
    assert(outputSchema.getAttributes.isEmpty)
  }

  it should "match drop names case-insensitively when deriving the schema" in {
    // Schema.remove lowercases both sides, matching the exec's behavior.
    val outputSchema = deriveSchema(isDrop = true)("FIELD2" -> "")
    assert(outputSchema.getAttributeNames == List("field1", "field3"))
  }

  it should "raise IllegalArgumentException on duplicate entries in the drop list" in {
    // The exec tolerates duplicates; the schema derivation folds
    // Schema.remove one entry at a time, so the second removal of the same name
    // rejects a now non-existent attribute.
    assertThrows[IllegalArgumentException] {
      deriveSchema(isDrop = true)("field2" -> "", "field2" -> "")
    }
  }

  it should "preserve a HashPartition when its attributes are non-empty" in {
    val out = projectionOpDesc.derivePartition()(List(HashPartition(List("field1"))))
    assert(out == HashPartition(List("field1")))
  }

  it should "downgrade an empty HashPartition to UnknownPartition" in {
    val out = projectionOpDesc.derivePartition()(List(HashPartition(List.empty)))
    assert(out == UnknownPartition())
  }

  it should "preserve a RangePartition when its attributes are non-empty" in {
    val out = projectionOpDesc.derivePartition()(List(RangePartition(List("field2"), 0L, 100L)))
    assert(out == RangePartition(List("field2"), 0L, 100L))
  }

  it should "downgrade an empty RangePartition to UnknownPartition" in {
    // RangePartition's companion apply already rewrites empty attributes to UnknownPartition,
    // so an empty range never reaches the range arm; either way the result is UnknownPartition.
    val out = projectionOpDesc.derivePartition()(List(RangePartition(List.empty, 0L, 100L)))
    assert(out == UnknownPartition())
  }

  it should "pass through partitions that are neither hash nor range" in {
    val out = projectionOpDesc.derivePartition()(List(SinglePartition()))
    assert(out == SinglePartition())
  }

  // Drop mode names the columns to remove; keep mode names the ones to hold on to,
  // in the order the user put them in.
  "ProjectionOpDesc.generateStandaloneCode" should "select or drop the named columns" in {
    val keep = new ProjectionOpDesc
    keep.attributes = units("a" -> "", "b" -> "")
    assert(keep.generateStandaloneCode().contains("""in1df[["a", "b"]]"""))

    val drop = new ProjectionOpDesc
    drop.attributes = units("a" -> "")
    drop.isDrop = true
    assert(drop.generateStandaloneCode() == """out1df = in1df.drop(columns=["a"])""")
  }

  // The engine reads `FIELD1` from a column `field1`, since Schema ignores case, and
  // names the kept column as the setting writes it. pandas would raise a KeyError on
  // `FIELD1`, so the script reads the column as the table spells it.
  it should "read a column named in a different case from the table" in {
    val inputSchemas = Map(PortIdentity() -> schema)

    val keep = new ProjectionOpDesc
    keep.attributes = units("FIELD1" -> "", "Field2" -> "f2")
    assert(
      keep.generateStandaloneCode(inputSchemas) ==
        """out1df = in1df[["field1", "field2"]].set_axis(["FIELD1", "f2"], axis=1)"""
    )

    val drop = new ProjectionOpDesc
    drop.attributes = units("FIELD1" -> "")
    drop.isDrop = true
    assert(
      drop.generateStandaloneCode(inputSchemas) == """out1df = in1df.drop(columns=["field1"])"""
    )
  }

  // `field1` as `first` and `FIELD1` as `second` read the same column. The engine
  // keeps both, in order; a rename map keyed by `field1` would keep only `second`,
  // twice. So the script names the output columns by position.
  it should "keep both aliases of one column read twice" in {
    val keep = new ProjectionOpDesc
    keep.attributes = units("field1" -> "first", "FIELD1" -> "second")
    assert(
      keep.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head.getAttributeNames ==
        List("first", "second")
    )
    assert(
      keep.generateStandaloneCode(Map(PortIdentity() -> schema)) ==
        """out1df = in1df[["field1", "field1"]].set_axis(["first", "second"], axis=1)"""
    )
  }

  // Schema propagation and the executor both refuse an empty selection, so the
  // script stops where a run would have. Passing the frame through would answer
  // with data a run never produces.
  it should "stop on an empty selection rather than pass the frame through" in {
    val code = (new ProjectionOpDesc).generateStandaloneCode()
    assert(code.startsWith("raise ValueError("))
    assert(code.contains("Please select at least one attribute to project."))
  }
}
