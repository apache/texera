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

package org.apache.texera.amber.core.tuple

import org.ehcache.sizeof.SizeOf
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp
import java.util.concurrent.{Callable, Executors, TimeUnit}

private[tuple] final class SizingTestAttribute(name: String)
    extends Attribute(name, AttributeType.STRING) {
  var extra: Array[Byte] = new Array[Byte](8)
}

private[tuple] final class SizingTestSchema(attrs: List[Attribute]) extends Schema(attrs) {
  var extra: Array[Byte] = new Array[Byte](8)
}

private[tuple] final class SizingTestLazyPayload {
  var evaluations = 0
  lazy val value: String = {
    evaluations += 1
    "evaluated"
  }
}

class TupleSpec extends AnyFlatSpec {
  val stringAttribute = new Attribute("col-string", AttributeType.STRING)
  val integerAttribute = new Attribute("col-int", AttributeType.INTEGER)
  val boolAttribute = new Attribute("col-bool", AttributeType.BOOLEAN)
  val longAttribute = new Attribute("col-long", AttributeType.LONG)
  val doubleAttribute = new Attribute("col-double", AttributeType.DOUBLE)
  val timestampAttribute = new Attribute("col-timestamp", AttributeType.TIMESTAMP)
  val binaryAttribute = new Attribute("col-binary", AttributeType.BINARY)

  val capitalizedStringAttribute = new Attribute("COL-string", AttributeType.STRING)

  it should "create a tuple with capitalized attributeName" in {

    val schema = Schema().add(capitalizedStringAttribute)
    val tuple = Tuple.builder(schema).add(capitalizedStringAttribute, "string-value").build()
    assert(tuple.getField("COL-string").asInstanceOf[String] == "string-value")

  }

  it should "create a tuple with capitalized attributeName, using addSequentially" in {
    val schema = Schema().add(capitalizedStringAttribute)
    val tuple = Tuple.builder(schema).addSequentially(Array("string-value")).build()
    assert(tuple.getField("COL-string").asInstanceOf[String] == "string-value")
  }

  it should "create a tuple using new builder, based on another tuple using old builder" in {
    val schema = Schema().add(stringAttribute)
    val inputTuple = Tuple.builder(schema).addSequentially(Array("string-value")).build()
    val newTuple = Tuple.builder(inputTuple.getSchema).add(inputTuple).build()

    assert(newTuple.length == inputTuple.length)
  }

  it should "fail when unknown attribute is added to tuple" in {
    val schema = Schema().add(stringAttribute)
    assertThrows[TupleBuildingException] {
      Tuple.builder(schema).add(integerAttribute, 1)
    }
  }

  it should "fail when tuple does not conform to complete schema" in {
    val schema = Schema().add(stringAttribute).add(integerAttribute)
    assertThrows[TupleBuildingException] {
      Tuple.builder(schema).add(integerAttribute, 1).build()
    }
  }

  it should "fail when entire tuple passed in has extra attributes" in {
    val inputSchema = Schema().add(stringAttribute).add(integerAttribute).add(boolAttribute)
    val inputTuple = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .build()

    val outputSchema = Schema().add(stringAttribute).add(integerAttribute)
    assertThrows[TupleBuildingException] {
      Tuple.builder(outputSchema).add(inputTuple).build()
    }
  }

  it should "not fail when entire tuple passed in has extra attributes and strictSchemaMatch is false" in {
    val inputSchema =
      Schema().add(stringAttribute).add(integerAttribute).add(boolAttribute)
    val inputTuple = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .build()

    val outputSchema = Schema().add(stringAttribute).add(integerAttribute)
    val outputTuple = Tuple.builder(outputSchema).add(inputTuple, false).build()

    // This is the important test. Input tuple has 3 attributes but output tuple has only 2
    // It's because of isStrictSchemaMatch=false
    assert(outputTuple.length == 2);
  }

  it should "calculate hash" in {
    val inputSchema =
      Schema()
        .add(integerAttribute)
        .add(stringAttribute)
        .add(boolAttribute)
        .add(longAttribute)
        .add(doubleAttribute)
        .add(timestampAttribute)
        .add(binaryAttribute)

    val inputTuple = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 922323)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .add(longAttribute, 1123213213213L)
      .add(doubleAttribute, 214214.9969346)
      .add(timestampAttribute, new Timestamp(100000000L))
      .add(binaryAttribute, Array[Byte](104, 101, 108, 108, 111))
      .build()
    assert(inputTuple.hashCode() == -1335416166)

    val inputTuple2 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 0)
      .add(stringAttribute, "")
      .add(boolAttribute, false)
      .add(longAttribute, 0L)
      .add(doubleAttribute, 0.0)
      .add(timestampAttribute, new Timestamp(0L))
      .add(binaryAttribute, Array[Byte]())
      .build()
    assert(inputTuple2.hashCode() == -1409761483)

    val inputTuple3 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, null)
      .add(stringAttribute, null)
      .add(boolAttribute, null)
      .add(longAttribute, null)
      .add(doubleAttribute, null)
      .add(timestampAttribute, null)
      .add(binaryAttribute, null)
      .build()
    assert(inputTuple3.hashCode() == 1742810335)

    val inputTuple4 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, -3245763)
      .add(stringAttribute, "\n\r\napple")
      .add(boolAttribute, true)
      .add(longAttribute, -8965536434247L)
      .add(doubleAttribute, 1 / 3.0d)
      .add(timestampAttribute, new Timestamp(-1990))
      .add(binaryAttribute, null)
      .build()
    assert(inputTuple4.hashCode() == -592643630)

    val inputTuple5 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, Int.MaxValue)
      .add(stringAttribute, new String())
      .add(boolAttribute, true)
      .add(longAttribute, Long.MaxValue)
      .add(doubleAttribute, 7 / 17.0d)
      .add(timestampAttribute, new Timestamp(1234567890L))
      .add(binaryAttribute, Array.fill[Byte](4097)('o'))
      .build()
    assert(inputTuple5.hashCode() == -2099556631)
  }

  it should "reject getField for an attribute name that is not in the tuple" in {
    val schema = Schema().add(stringAttribute)
    val tuple = Tuple.builder(schema).add(stringAttribute, "v").build()
    val ex = intercept[RuntimeException] {
      tuple.getField[String]("col-missing")
    }
    assert(ex.getMessage == "col-missing is not in the tuple")
  }

  it should "get a field by Attribute" in {
    val schema = Schema().add(stringAttribute)
    val tuple = Tuple.builder(schema).add(stringAttribute, "v").build()
    assert(tuple.getField[String](stringAttribute) == "v")
  }

  it should "enforce its own schema and reject a different one" in {
    val schema = Schema().add(stringAttribute)
    val tuple = Tuple.builder(schema).add(stringAttribute, "v").build()
    assert(tuple.enforceSchema(schema) eq tuple)
    val ex = intercept[AssertionError] {
      tuple.enforceSchema(Schema().add(integerAttribute))
    }
    assert(ex.getMessage.contains("output tuple schema does not match the expected schema!"))
  }

  it should "compare tuples by value, including binary contents" in {
    val schema = Schema().add(stringAttribute).add(binaryAttribute)
    def tupleWith(s: String): Tuple =
      Tuple
        .builder(schema)
        .add(stringAttribute, s)
        .add(binaryAttribute, Array[Byte](1, 2, 3))
        .build()
    val tupleA = tupleWith("v")
    val tupleB = tupleWith("v")
    assert(tupleA == tupleB) // distinct Array[Byte] instances, same contents
    val differentField = tupleWith("other")
    assert(tupleA != differentField)
    val differentSchema =
      Tuple.builder(Schema().add(stringAttribute)).add(stringAttribute, "v").build()
    assert(tupleA != differentSchema)
    assert(!tupleA.equals("not a tuple"))
    assert(!tupleA.equals(null))
  }

  it should "build a partial tuple following the requested attribute order" in {
    val schema = Schema().add(stringAttribute).add(integerAttribute).add(boolAttribute)
    val tuple = Tuple
      .builder(schema)
      .add(stringAttribute, "s")
      .add(integerAttribute, 1)
      .add(boolAttribute, true)
      .build()
    val partial = tuple.getPartialTuple(List("col-bool", "col-string"))
    assert(partial.length == 2)
    assert(partial.getField[Boolean]("col-bool"))
    assert(partial.getField[String]("col-string") == "s")
    assert(partial.getSchema == Schema().add(boolAttribute).add(stringAttribute))
  }

  it should "render toString with the schema and field values" in {
    val schema = Schema().add(stringAttribute)
    val tuple = Tuple.builder(schema).add(stringAttribute, "v").build()
    assert(tuple.toString == s"Tuple [schema=$schema, fields=[v]]")
  }

  it should "reject construction when schema and field sizes differ" in {
    val schema = Schema().add(stringAttribute)
    val ex = intercept[RuntimeException] {
      Tuple(schema, Array[Any]("a", "b"))
    }
    assert(ex.getMessage == "Schema size (1) and field size (2) are different")
  }

  it should "reject a field whose class does not match the attribute type" in {
    val ex = intercept[RuntimeException] {
      Tuple.builder(Schema().add(stringAttribute)).add(stringAttribute, Integer.valueOf(1))
    }
    assert(
      ex.getMessage ==
        "Attribute col-string's type (string) is different from field's type (integer)"
    )
  }

  it should "accept any field class for an ANY-typed attribute" in {
    val anyAttribute = new Attribute("col-any", AttributeType.ANY)
    val tuple =
      Tuple.builder(Schema().add(anyAttribute)).add(anyAttribute, List(1, 2, 3)).build()
    assert(tuple.getField[List[Int]]("col-any") == List(1, 2, 3))
  }

  it should "support the three-argument builder add and reject null arguments" in {
    val schema = Schema().add(stringAttribute)
    val tuple =
      Tuple.builder(schema).add("col-string", AttributeType.STRING, "v").build()
    assert(tuple.getField[String]("col-string") == "v")
    intercept[IllegalArgumentException] {
      Tuple.builder(schema).add(null.asInstanceOf[String], null, "v")
    }
    intercept[IllegalArgumentException] {
      Tuple.builder(schema).add(null.asInstanceOf[Attribute], "v")
    }
    intercept[IllegalArgumentException] {
      Tuple.builder(schema).add(null.asInstanceOf[Tuple])
    }
    intercept[IllegalArgumentException] {
      Tuple.builder(schema).addSequentially(null)
    }
  }

  it should "expose the case-class members of Tuple" in {
    val schema = Schema().add(stringAttribute)
    val tuple = Tuple(schema, Array[Any]("hello"))
    val copied = tuple.copy()
    assert(copied == tuple)
    assert(tuple.canEqual(copied))
    assert(tuple.productArity == 2)
    assert(tuple.productElement(0) == schema)
    val Tuple(unappliedSchema, _) = tuple
    assert(unappliedSchema == schema)
  }

  it should "reject construction with a null schema or a null field array" in {
    // both arguments are null-checked before any schema/field matching happens
    intercept[NullPointerException] {
      Tuple(null, Array[Any]("a"))
    }
    intercept[NullPointerException] {
      Tuple(Schema().add(stringAttribute), null)
    }
  }

  it should "read fields by positional index and fail outside the field array" in {
    val schema = Schema().add(stringAttribute).add(integerAttribute)
    val tuple = Tuple(schema, Array[Any]("s", Integer.valueOf(7)))
    assert(tuple.getField[String](0) == "s")
    assert(tuple.getField[Int](1) == 7)
    assert(tuple.length == 2)
    intercept[ArrayIndexOutOfBoundsException] {
      tuple.getField[String](2)
    }
  }

  it should "compute an in-memory size that grows with the payload" in {
    val schema = Schema().add(stringAttribute)
    val small = Tuple(schema, Array[Any]("a"))
    val large = Tuple(schema, Array[Any]("a" * 10000))
    assert(small.inMemSize > 0)
    assert(large.inMemSize > small.inMemSize)
  }

  // A tuple's inMemSize is the byte charge flow control and statistics use. These tests
  // compare it with what a brand-new SizeOf engine reports for the same tuple, so they
  // do not depend on the JVM's object layout or the sizeof version.
  private def freshCharge(tuple: Tuple): Long = SizeOf.newInstance().deepSizeOf(tuple)

  private def assertFreshCharge(tuple: Tuple): Unit = {
    val expected = freshCharge(tuple)
    assert(
      tuple.inMemSize == expected,
      s"charged ${tuple.inMemSize} bytes, a fresh SizeOf walk reports $expected, " +
        s"schema ${tuple.getSchema}"
    )
  }

  it should "charge what a fresh SizeOf walk reports, for every attribute type" in {
    val largeBinaryAttribute = new Attribute("col-large-binary", AttributeType.LARGE_BINARY)
    val anyAttribute = new Attribute("col-any", AttributeType.ANY)
    val schema = Schema()
      .add(stringAttribute)
      .add(integerAttribute)
      .add(boolAttribute)
      .add(longAttribute)
      .add(doubleAttribute)
      .add(timestampAttribute)
      .add(binaryAttribute)
      .add(largeBinaryAttribute)
      .add(anyAttribute)
    assert(schema.getAttributes.map(_.getType).toSet == AttributeType.values().toSet)

    assertFreshCharge(
      Tuple(
        schema,
        Array[Any](
          "雪-😀 café",
          Integer.valueOf(1000),
          java.lang.Boolean.TRUE,
          java.lang.Long.valueOf(Long.MaxValue),
          java.lang.Double.valueOf(1.25),
          new Timestamp(123456789L),
          Array[Byte](0, 1, -1),
          new LargeBinary("s3://bucket/key"),
          List(1, 2, 3)
        )
      )
    )
    assertFreshCharge(Tuple(schema, Array.fill[Any](schema.getAttributes.size)(null)))
    assertFreshCharge(Tuple(Schema(), Array.empty[Any]))
    assertFreshCharge(Tuple(Schema().add(stringAttribute), Array[Any]("漢字🧪" * 20000)))
    assertFreshCharge(Tuple(Schema().add(binaryAttribute), Array[Any](new Array[Byte](262145))))

    // Displaying a Timestamp fills its cached calendar fields, which the size must include.
    val displayed = new Timestamp(123456789L)
    assert(displayed.toString.nonEmpty)
    assertFreshCharge(Tuple(Schema().add(timestampAttribute), Array[Any](displayed)))
  }

  it should "count an object shared within a tuple once, and charge a rebuilt tuple the same" in {
    // The column name doubles as a value, and one byte array fills two fields.
    val name = new String("col-shared-雪".toCharArray)
    val bytes = Array.fill[Byte](8193)(42)
    val schema = Schema()
      .add(new Attribute(name, AttributeType.STRING))
      .add(binaryAttribute)
      .add(new Attribute("col-binary-copy", AttributeType.BINARY))

    val shared = Tuple(schema, Array[Any](name, bytes, bytes))
    val copied = Tuple(schema, Array[Any](new String(name), bytes, bytes.clone()))
    assertFreshCharge(shared)
    assertFreshCharge(copied)
    assert(shared.inMemSize < copied.inMemSize)

    // Nothing one sizing walk sees may carry over into the next one.
    val rebuilt = Tuple(schema, Array[Any](name, bytes, bytes))
    assert(rebuilt.inMemSize == shared.inMemSize)
  }

  it should "size ANY values with cycles and nested tuples without evaluating lazy members" in {
    val anyAttribute = new Attribute("col-any", AttributeType.ANY)
    val schema = Schema().add(anyAttribute)

    val cycle = new java.util.ArrayList[AnyRef]()
    cycle.add(cycle)
    cycle.add(schema)
    assertFreshCharge(Tuple(schema, Array[Any](cycle)))

    val nested = Tuple(Schema().add(stringAttribute), Array[Any]("nested"))
    assertFreshCharge(Tuple(schema, Array[Any](nested)))

    val payload = new SizingTestLazyPayload
    assertFreshCharge(Tuple(schema, Array[Any](payload)))
    assert(payload.evaluations == 0)
  }

  it should "keep the size charged at construction after its field array changes" in {
    val fields = Array[Any](new Array[Byte](16))
    val tuple = Tuple(Schema().add(binaryAttribute), fields)
    // Walk the tuple before the change without reading inMemSize, so a charge computed
    // later than construction would show up as a mismatch below.
    val beforeChange = freshCharge(tuple)

    fields(0) = new Array[Byte](16384)
    assert(tuple.inMemSize == beforeChange)
    assert(freshCharge(tuple) > beforeChange)
    assertFreshCharge(Tuple(tuple.getSchema, fields))
  }

  it should "include state added by Attribute and Schema subclasses as of each construction" in {
    val attribute = new SizingTestAttribute("col-sized")
    val schema = Schema(List(attribute))
    val before = Tuple(schema, Array[Any]("v"))
    attribute.extra = new Array[Byte](16385)
    val after = Tuple(schema, Array[Any]("v"))
    assertFreshCharge(after)
    assert(after.inMemSize > before.inMemSize)

    val sizedSchema = new SizingTestSchema(List(stringAttribute))
    val small = Tuple(sizedSchema, Array[Any]("v"))
    sizedSchema.extra = new Array[Byte](32769)
    val large = Tuple(sizedSchema, Array[Any]("v"))
    assertFreshCharge(large)
    assert(large.inMemSize > small.inMemSize)
  }

  it should "charge tuples built concurrently on many threads what a fresh SizeOf walk reports" in {
    val sharedSchema = Schema().add(stringAttribute).add(binaryAttribute)
    val threads = 8
    val rowsPerThread = 200
    val pool = Executors.newFixedThreadPool(threads)
    try {
      val jobs = (0 until threads).map { worker =>
        pool.submit(new Callable[Seq[String]] {
          override def call(): Seq[String] =
            (0 until rowsPerThread).flatMap { row =>
              // Even rows share one schema across threads; odd rows bring a new schema.
              val tuple =
                if (row % 2 == 0)
                  Tuple(sharedSchema, Array[Any](s"w$worker-r$row-雪", new Array[Byte](row)))
                else
                  Tuple(
                    Schema().add(s"col-$worker-$row", AttributeType.LONG),
                    Array[Any](java.lang.Long.valueOf(row.toLong))
                  )
              val expected = freshCharge(tuple)
              if (tuple.inMemSize == expected) None
              else Some(s"worker $worker row $row: ${tuple.inMemSize} != $expected")
            }
        })
      }
      val mismatches = jobs.flatMap(_.get(30, TimeUnit.SECONDS))
      assert(mismatches.isEmpty, mismatches.take(5).mkString("; "))
    } finally {
      pool.shutdownNow()
    }
  }

  it should "name the missing attributes when the builder is incomplete" in {
    val schema = Schema().add(stringAttribute).add(integerAttribute).add(boolAttribute)
    val ex = intercept[TupleBuildingException] {
      Tuple.builder(schema).add(stringAttribute, "s").build()
    }
    assert(ex.getMessage.startsWith("Tuple does not have the same number of attributes as schema."))
    assert(ex.getMessage.contains("col-int"))
    assert(ex.getMessage.contains("col-bool"))
    assert(!ex.getMessage.contains("col-string"))
  }

  it should "let a later add overwrite the value of the same attribute" in {
    val schema = Schema().add(stringAttribute)
    val tuple = Tuple
      .builder(schema)
      .add(stringAttribute, "first")
      .add(stringAttribute, "second")
      .build()
    assert(tuple.length == 1)
    assert(tuple.getField[String]("col-string") == "second")
  }

  it should "reject addSequentially when the field count differs from the schema" in {
    val schema = Schema().add(stringAttribute).add(integerAttribute)
    val tooFew = intercept[RuntimeException] {
      Tuple.builder(schema).addSequentially(Array[Any]("only-one"))
    }
    assert(tooFew.getMessage == "Schema size (2) and field size (1) are different")
    val tooMany = intercept[RuntimeException] {
      Tuple.builder(schema).addSequentially(Array[Any]("a", Integer.valueOf(1), "extra"))
    }
    assert(tooMany.getMessage == "Schema size (2) and field size (3) are different")
  }

  it should "still require every schema attribute when adding a tuple non-strictly" in {
    // non-strict mode only skips *extra* source attributes; it does not fill gaps
    val sourceSchema = Schema().add(stringAttribute)
    val source = Tuple.builder(sourceSchema).add(stringAttribute, "s").build()
    val targetSchema = Schema().add(stringAttribute).add(integerAttribute)
    val ex = intercept[TupleBuildingException] {
      Tuple.builder(targetSchema).add(source, false).build()
    }
    assert(ex.getMessage.contains("col-int"))
  }

  it should "distinguish binary fields whose contents differ" in {
    val schema = Schema().add(binaryAttribute)
    def withBytes(bytes: Array[Byte]): Tuple =
      Tuple.builder(schema).add(binaryAttribute, bytes).build()

    val base = withBytes(Array[Byte](1, 2, 3))
    assert(base == withBytes(Array[Byte](1, 2, 3)))
    assert(base.hashCode() == withBytes(Array[Byte](1, 2, 3)).hashCode())
    assert(base != withBytes(Array[Byte](1, 2, 4)))
    assert(base != withBytes(Array[Byte](1, 2)))
    assert(base != withBytes(Array[Byte]()))
  }

  it should "reject a partial tuple for an attribute outside the schema" in {
    val schema = Schema().add(stringAttribute)
    val tuple = Tuple.builder(schema).add(stringAttribute, "s").build()
    intercept[RuntimeException] {
      tuple.getPartialTuple(List("col-missing"))
    }
  }

  it should "preserve valid fields and compare null vs binary fields safely across equality and non-strict builds" in {
    val binSchema = Schema().add(stringAttribute).add(binaryAttribute)
    val withNull =
      Tuple.builder(binSchema).add(stringAttribute, "a").add(binaryAttribute, null).build()
    val otherNull =
      Tuple.builder(binSchema).add(stringAttribute, "a").add(binaryAttribute, null).build()
    val withBytes =
      Tuple
        .builder(binSchema)
        .add(stringAttribute, "a")
        .add(binaryAttribute, Array[Byte](1, 2))
        .build()
    val diffString =
      Tuple
        .builder(binSchema)
        .add(stringAttribute, "b")
        .add(binaryAttribute, Array[Byte](1, 2))
        .build()

    assert(withNull == otherNull)
    assert(withNull != withBytes)
    assert(withBytes != withNull)
    assert(withBytes != diffString)

    val narrowedSchema = Schema().add(stringAttribute)
    val nonStrict =
      Tuple.builder(narrowedSchema).add(withBytes, isStrictSchemaMatch = false).build()
    assert(nonStrict.getSchema == narrowedSchema)
    assert(nonStrict.getField[String]("col-string") == "a")

    val reordered = withBytes.getPartialTuple(List("col-binary", "col-string"))
    assert(reordered.getField[Array[Byte]](0).sameElements(Array[Byte](1, 2)))
    assert(reordered.getField[String](1) == "a")
  }
}
