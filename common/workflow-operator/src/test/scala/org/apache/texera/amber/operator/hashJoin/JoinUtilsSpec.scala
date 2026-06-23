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

package org.apache.texera.amber.operator.hashJoin

import org.apache.texera.amber.core.tuple.{
  Attribute,
  AttributeType,
  Schema,
  SchemaEnforceable,
  Tuple
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JoinUtilsSpec extends AnyFlatSpec with Matchers {

  private def schemaOf(attrs: (String, AttributeType)*): Schema =
    attrs.foldLeft(Schema())((s, a) => s.add(new Attribute(a._1, a._2)))

  private def tupleOf(schema: Schema, values: (String, Any)*): Tuple = {
    val b = Tuple.builder(schema)
    values.foreach { case (name, v) => b.add(schema.getAttribute(name), v) }
    b.build()
  }

  "JoinUtils.joinTuples" should "concatenate the left and right tuple fields" in {
    val leftSchema = schemaOf("a" -> AttributeType.STRING, "b" -> AttributeType.INTEGER)
    val rightSchema = schemaOf("c" -> AttributeType.STRING)
    val joined = JoinUtils.joinTuples(
      tupleOf(leftSchema, "a" -> "av", "b" -> Integer.valueOf(1)),
      tupleOf(rightSchema, "c" -> "cv")
    )
    joined.getFields.length shouldBe 3
    val enforced = joined
      .asInstanceOf[SchemaEnforceable]
      .enforceSchema(
        schemaOf(
          "a" -> AttributeType.STRING,
          "b" -> AttributeType.INTEGER,
          "c" -> AttributeType.STRING
        )
      )
    enforced.getField[String]("a") shouldBe "av"
    enforced.getField[String]("c") shouldBe "cv"
  }

  it should "skip the named attribute (the join key) from the right tuple" in {
    val leftSchema = schemaOf("a" -> AttributeType.STRING)
    val rightSchema = schemaOf("k" -> AttributeType.STRING, "c" -> AttributeType.STRING)
    val joined = JoinUtils.joinTuples(
      tupleOf(leftSchema, "a" -> "av"),
      tupleOf(rightSchema, "k" -> "kv", "c" -> "cv"),
      skipAttributeName = Some("k")
    )
    joined.getFields.length shouldBe 2
    val enforced = joined
      .asInstanceOf[SchemaEnforceable]
      .enforceSchema(schemaOf("a" -> AttributeType.STRING, "c" -> AttributeType.STRING))
    enforced.getField[String]("a") shouldBe "av"
    enforced.getField[String]("c") shouldBe "cv"
  }

  it should "rename a right-side name conflict with a #@1 suffix" in {
    val schema = schemaOf("x" -> AttributeType.STRING)
    val joined = JoinUtils.joinTuples(
      tupleOf(schema, "x" -> "L"),
      tupleOf(schema, "x" -> "R")
    )
    joined.getFields.length shouldBe 2
    val enforced = joined
      .asInstanceOf[SchemaEnforceable]
      .enforceSchema(schemaOf("x" -> AttributeType.STRING, "x#@1" -> AttributeType.STRING))
    enforced.getField[String]("x") shouldBe "L"
    enforced.getField[String]("x#@1") shouldBe "R"
  }
}
