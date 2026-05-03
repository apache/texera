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

package org.apache.texera.amber.util

import org.apache.arrow.vector.types.{FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.AttributeTypeException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ArrowUtilsSpec extends AnyFlatSpec with Matchers {

  // ----- toAttributeType -----

  "toAttributeType" should "map Int(16) to INTEGER" in {
    ArrowUtils.toAttributeType(new ArrowType.Int(16, true)) shouldBe AttributeType.INTEGER
  }

  it should "map Int(32) to INTEGER" in {
    ArrowUtils.toAttributeType(new ArrowType.Int(32, true)) shouldBe AttributeType.INTEGER
  }

  it should "map Int(64) to LONG" in {
    ArrowUtils.toAttributeType(new ArrowType.Int(64, true)) shouldBe AttributeType.LONG
  }

  it should "map non-standard Int bit-widths to LONG (current behavior)" in {
    // Pin: the source code's match is `case 16 | 32 => INTEGER` then
    // `case 64 | _ => LONG`. The trailing `_` makes the second arm a
    // catch-all, so Int(8), Int(128) and any other width all surface as
    // LONG. A future fix that distinguishes those widths will deliberately
    // break this spec.
    ArrowUtils.toAttributeType(new ArrowType.Int(8, true)) shouldBe AttributeType.LONG
    ArrowUtils.toAttributeType(new ArrowType.Int(128, true)) shouldBe AttributeType.LONG
  }

  it should "map Bool to BOOLEAN" in {
    ArrowUtils.toAttributeType(ArrowType.Bool.INSTANCE) shouldBe AttributeType.BOOLEAN
  }

  it should "map FloatingPoint to DOUBLE" in {
    ArrowUtils.toAttributeType(
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    ) shouldBe AttributeType.DOUBLE
  }

  it should "map Timestamp to TIMESTAMP" in {
    ArrowUtils.toAttributeType(
      new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")
    ) shouldBe AttributeType.TIMESTAMP
  }

  it should "map Utf8 to STRING" in {
    ArrowUtils.toAttributeType(ArrowType.Utf8.INSTANCE) shouldBe AttributeType.STRING
  }

  it should "map Binary to BINARY" in {
    ArrowUtils.toAttributeType(new ArrowType.Binary) shouldBe AttributeType.BINARY
  }

  it should "throw AttributeTypeException for unsupported Arrow types" in {
    // ArrowType.Null is a real Arrow type that this method doesn't handle.
    assertThrows[AttributeTypeException] {
      ArrowUtils.toAttributeType(ArrowType.Null.INSTANCE)
    }
  }

  // ----- fromAttributeType -----

  "fromAttributeType" should "map INTEGER to Int(32, signed)" in {
    val arrow = ArrowUtils.fromAttributeType(AttributeType.INTEGER)
    arrow shouldBe new ArrowType.Int(32, true)
  }

  it should "map LONG to Int(64, signed)" in {
    val arrow = ArrowUtils.fromAttributeType(AttributeType.LONG)
    arrow shouldBe new ArrowType.Int(64, true)
  }

  it should "map DOUBLE to FloatingPoint(DOUBLE)" in {
    val arrow = ArrowUtils.fromAttributeType(AttributeType.DOUBLE)
    arrow shouldBe new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
  }

  it should "map BOOLEAN to Bool.INSTANCE" in {
    ArrowUtils.fromAttributeType(AttributeType.BOOLEAN) shouldBe ArrowType.Bool.INSTANCE
  }

  it should "map TIMESTAMP to Timestamp(MILLISECOND, UTC)" in {
    val arrow = ArrowUtils.fromAttributeType(AttributeType.TIMESTAMP)
    arrow shouldBe new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")
  }

  it should "map BINARY to ArrowType.Binary" in {
    ArrowUtils.fromAttributeType(AttributeType.BINARY) shouldBe new ArrowType.Binary
  }

  it should "map STRING, LARGE_BINARY, and ANY all to Utf8.INSTANCE" in {
    // Pin: STRING / LARGE_BINARY / ANY collapse onto the same Arrow type
    // (Utf8). LARGE_BINARY is recovered via field metadata, ANY loses its
    // distinction entirely. Documenting the collision so a future change
    // that splits them apart will surface here.
    ArrowUtils.fromAttributeType(AttributeType.STRING) shouldBe ArrowType.Utf8.INSTANCE
    ArrowUtils.fromAttributeType(
      AttributeType.LARGE_BINARY
    ) shouldBe ArrowType.Utf8.INSTANCE
    ArrowUtils.fromAttributeType(AttributeType.ANY) shouldBe ArrowType.Utf8.INSTANCE
  }

  // ----- bool2int implicit -----

  "bool2int implicit" should "map true to 1 and false to 0" in {
    import ArrowUtils.bool2int
    val one: Int = true
    val zero: Int = false
    one shouldBe 1
    zero shouldBe 0
  }

  // ----- toTexeraSchema -----

  private def arrowField(
      name: String,
      t: ArrowType,
      metadata: util.Map[String, String] = null
  ): Field =
    new Field(name, new FieldType(true, t, null, metadata), null)

  "toTexeraSchema" should "produce an empty Texera Schema for an empty Arrow schema" in {
    val arrow = new org.apache.arrow.vector.types.pojo.Schema(util.Arrays.asList[Field]())
    ArrowUtils.toTexeraSchema(arrow).getAttributes shouldBe empty
  }

  it should "translate each Arrow field to a Texera Attribute by primitive type" in {
    val arrow = new org.apache.arrow.vector.types.pojo.Schema(
      util.Arrays.asList(
        arrowField("a", new ArrowType.Int(32, true)),
        arrowField("b", new ArrowType.Int(64, true)),
        arrowField("c", ArrowType.Bool.INSTANCE),
        arrowField("d", ArrowType.Utf8.INSTANCE)
      )
    )
    val schema = ArrowUtils.toTexeraSchema(arrow)
    val attrs = schema.getAttributes.toList
    attrs.map(_.getName) shouldBe List("a", "b", "c", "d")
    attrs.map(_.getType) shouldBe List(
      AttributeType.INTEGER,
      AttributeType.LONG,
      AttributeType.BOOLEAN,
      AttributeType.STRING
    )
  }

  it should "promote Utf8 fields to LARGE_BINARY when texera_type metadata says so" in {
    val md = new util.HashMap[String, String]()
    md.put("texera_type", "LARGE_BINARY")
    val arrow = new org.apache.arrow.vector.types.pojo.Schema(
      util.Arrays.asList(
        arrowField("blob", ArrowType.Utf8.INSTANCE, md),
        arrowField("plain", ArrowType.Utf8.INSTANCE) // no metadata
      )
    )
    val schema = ArrowUtils.toTexeraSchema(arrow)
    val attrs = schema.getAttributes.toList
    attrs.map(_.getName) shouldBe List("blob", "plain")
    attrs.map(_.getType) shouldBe List(AttributeType.LARGE_BINARY, AttributeType.STRING)
  }

  // ----- fromTexeraSchema -----

  "fromTexeraSchema" should "translate each Texera Attribute to an Arrow field with primitive types" in {
    val schema = Schema(
      List(
        new Attribute("i", AttributeType.INTEGER),
        new Attribute("l", AttributeType.LONG),
        new Attribute("d", AttributeType.DOUBLE),
        new Attribute("b", AttributeType.BOOLEAN)
      )
    )
    val arrow = ArrowUtils.fromTexeraSchema(schema)
    val fields = arrow.getFields.asScala.toList
    fields.map(_.getName) shouldBe List("i", "l", "d", "b")
    fields.map(_.getFieldType.getType) shouldBe List(
      new ArrowType.Int(32, true),
      new ArrowType.Int(64, true),
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
      ArrowType.Bool.INSTANCE
    )
  }

  it should "attach texera_type=LARGE_BINARY metadata to LARGE_BINARY fields and only those" in {
    val schema = Schema(
      List(
        new Attribute("blob", AttributeType.LARGE_BINARY),
        new Attribute("name", AttributeType.STRING)
      )
    )
    val arrow = ArrowUtils.fromTexeraSchema(schema)
    val fields = arrow.getFields.asScala.toList
    val blob = fields.find(_.getName == "blob").get
    val name = fields.find(_.getName == "name").get
    blob.getMetadata.get("texera_type") shouldBe "LARGE_BINARY"
    // STRING fields do not get the texera_type metadata.
    Option(name.getMetadata).map(_.containsKey("texera_type")).getOrElse(false) shouldBe false
  }

  // ----- round-trip -----

  "schema round-trip" should "preserve primitive AttributeTypes through fromTexeraSchema and back" in {
    val original = Schema(
      List(
        new Attribute("i", AttributeType.INTEGER),
        new Attribute("l", AttributeType.LONG),
        new Attribute("d", AttributeType.DOUBLE),
        new Attribute("b", AttributeType.BOOLEAN),
        new Attribute("t", AttributeType.TIMESTAMP),
        new Attribute("s", AttributeType.STRING),
        new Attribute("y", AttributeType.BINARY)
      )
    )
    val recovered = ArrowUtils.toTexeraSchema(ArrowUtils.fromTexeraSchema(original))
    recovered.getAttributes.toList.map(a => (a.getName, a.getType)) shouldBe
      original.getAttributes.toList.map(a => (a.getName, a.getType))
  }

  it should "preserve LARGE_BINARY through the metadata-based path" in {
    val original = Schema(
      List(
        new Attribute("blob", AttributeType.LARGE_BINARY),
        new Attribute("name", AttributeType.STRING)
      )
    )
    val recovered = ArrowUtils.toTexeraSchema(ArrowUtils.fromTexeraSchema(original))
    recovered.getAttributes.toList.map(a => (a.getName, a.getType)) shouldBe List(
      ("blob", AttributeType.LARGE_BINARY),
      ("name", AttributeType.STRING)
    )
  }

  it should "lose the ANY distinction (round-trips as STRING)" in {
    // Pin: ANY fromAttributeType produces Utf8 with no metadata. toAttributeType
    // then can only see Utf8, so the recovered type is STRING. Documenting this
    // information loss so a future fix that round-trips ANY can break the spec.
    val original = Schema(List(new Attribute("v", AttributeType.ANY)))
    val recovered = ArrowUtils.toTexeraSchema(ArrowUtils.fromTexeraSchema(original))
    recovered.getAttributes.toList.map(a => (a.getName, a.getType)) shouldBe List(
      ("v", AttributeType.STRING)
    )
  }
}
