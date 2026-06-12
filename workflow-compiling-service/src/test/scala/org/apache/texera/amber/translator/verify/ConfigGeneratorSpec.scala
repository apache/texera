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

package org.apache.texera.amber.translator.verify

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.operator.filter.SpecializedFilterOpDesc
import org.apache.texera.amber.operator.hashJoin.HashJoinOpDesc
import org.apache.texera.amber.operator.intersect.IntersectOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Drives [[ConfigGenerator]] across the range of operator config shapes:
  * no-config, flat autofill + enum, and a nested list of objects with a
  * free-form value. These are the cases the reflective generator must handle to
  * cover the JVM-exec operators automatically.
  */
class ConfigGeneratorSpec extends AnyFlatSpec with Matchers {

  private val schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING),
    new Attribute("score", AttributeType.DOUBLE)
  )
  private val twoPorts = Map(0 -> schema, 1 -> schema)

  "ConfigGenerator" should "configure an operator that has no config fields" in {
    val result = ConfigGenerator.generate(classOf[IntersectOpDesc], twoPorts)
    withClue(result) { result.isRight shouldBe true }
    result.toOption.get shouldBe a[IntersectOpDesc]
  }

  it should "fill autofill column refs from the correct port and default the enum" in {
    val result = ConfigGenerator.generate(classOf[HashJoinOpDesc[Any]], twoPorts)
    withClue(result) { result.isRight shouldBe true }
    val op = result.toOption.get.asInstanceOf[HashJoinOpDesc[Any]]
    schema.getAttributeNames should contain(op.buildAttributeName)
    schema.getAttributeNames should contain(op.probeAttributeName)
    op.joinType should not be null
  }

  it should "build a non-empty nested predicate list with a valid column and enum" in {
    val result = ConfigGenerator.generate(classOf[SpecializedFilterOpDesc], Map(0 -> schema))
    result.isRight shouldBe true
    val op = result.toOption.get.asInstanceOf[SpecializedFilterOpDesc]
    op.predicates should not be empty
    val p = op.predicates.head
    schema.getAttributeNames should contain(p.attribute)
    p.condition should not be null
  }
}
