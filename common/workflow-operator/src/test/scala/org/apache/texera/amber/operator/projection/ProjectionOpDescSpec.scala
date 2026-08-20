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

  it should "take in attribute names" in {
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field1", "f1"),
      new AttributeUnit("fields2", "f2")
    )

    assert(projectionOpDesc.attributes.length == 2)

  }

  it should "filter schema correctly" in {
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field1", "f1"),
      new AttributeUnit("field2", "f2")
    )
    val outputSchema =
      projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    assert(outputSchema.getAttributes.length == 2)

  }

  it should "reorder schema" in {
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field2", "f2"),
      new AttributeUnit("field1", "f1")
    )
    val outputSchema =
      projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    assert(outputSchema.getAttributes.length == 2)
    assert(outputSchema.getIndex("f2") == 0)
    assert(outputSchema.getIndex("f1") == 1)

  }

  it should "raise RuntimeException on non-existing fields" in {
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field---5", "f5"),
      new AttributeUnit("field---6", "f6")
    )
    assertThrows[RuntimeException] {
      projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    }

  }

  it should "raise IllegalArgumentException on empty attributes" in {

    assertThrows[IllegalArgumentException] {
      projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    }

  }

  it should "raise RuntimeException on duplicate alias" in {

    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field2", "f"),
      new AttributeUnit("field1", "f")
    )
    assertThrows[RuntimeException] {
      projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    }
  }

  it should "allow alias to be optional" in {
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field1", "f1"),
      new AttributeUnit("field2", "")
    )
    val outputSchema =
      projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    assert(outputSchema.getAttributes.length == 2)

  }

  it should "derive the drop-mode schema with original names, types and order" in {
    projectionOpDesc.isDrop = true
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field2", "")
    )
    val outputSchema =
      projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    assert(outputSchema.getAttributes.length == 2)
    assert(outputSchema.getIndex("field1") == 0)
    assert(outputSchema.getIndex("field3") == 1)
    assert(outputSchema.getAttribute("field1").getType == AttributeType.STRING)
    assert(outputSchema.getAttribute("field3").getType == AttributeType.BOOLEAN)
  }

  it should "ignore aliases when deriving the drop-mode schema" in {
    projectionOpDesc.isDrop = true
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field1", "renamed")
    )
    val outputSchema =
      projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    assert(outputSchema.getAttributeNames == List("field2", "field3"))
  }

  it should "raise IllegalArgumentException when dropping a non-existent attribute" in {
    // Unlike the exec, whose diff-based rewrite silently ignores unknown names,
    // Schema.remove rejects them at schema-derivation time.
    projectionOpDesc.isDrop = true
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field---5", "f5")
    )
    assertThrows[IllegalArgumentException] {
      projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    }
  }

  it should "derive an empty schema when dropping every attribute" in {
    projectionOpDesc.isDrop = true
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field1", ""),
      new AttributeUnit("field2", ""),
      new AttributeUnit("field3", "")
    )
    val outputSchema =
      projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    assert(outputSchema.getAttributes.isEmpty)
  }

  it should "match drop names case-insensitively when deriving the schema" in {
    // Unlike the exec, whose diff-based rewrite matches names exactly and
    // would keep field2, Schema.remove lowercases both sides.
    projectionOpDesc.isDrop = true
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("FIELD2", "")
    )
    val outputSchema =
      projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
    assert(outputSchema.getAttributeNames == List("field1", "field3"))
  }

  it should "raise IllegalArgumentException on duplicate entries in the drop list" in {
    // The exec's multiset diff tolerates duplicates; the schema derivation folds
    // Schema.remove one entry at a time, so the second removal of the same name
    // rejects a now non-existent attribute.
    projectionOpDesc.isDrop = true
    projectionOpDesc.attributes ++= List(
      new AttributeUnit("field2", ""),
      new AttributeUnit("field2", "")
    )
    assertThrows[IllegalArgumentException] {
      projectionOpDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head
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

}
