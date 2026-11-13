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

package org.apache.texera.amber.operator.metadata

import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
  * Guard for `attributeTypeRules`, over every registered operator.
  *
  * The property editor looks a rule's key up among the enclosing object's properties,
  * by the `@JsonProperty` name. A key that names no property, or a rule that is not an
  * object, makes the editor's constraint check return without checking anything -- the
  * rule reads as enforced while enforcing nothing, and no existing test notices,
  * because the operator still compiles and still runs.
  *
  * Sweeping every operator rather than the ones a fix happened to touch is deliberate:
  * the mistake is invisible at the declaration site, so it is as easy to make in the
  * next operator as it was in these.
  */
class AttributeTypeRuleTargetSpec extends AnyFlatSpec with Matchers {

  private val RulesKeyword = "attributeTypeRules"

  /** One declared rule, and the properties it could legally have named. */
  private case class Rule(
      schemaPath: String,
      key: String,
      value: JsonNode,
      declaredProperties: Set[String]
  ) {
    override def toString: String =
      s"$schemaPath.$RulesKeyword.$key " +
        s"(declared properties at $schemaPath: ${declaredProperties.toSeq.sorted.mkString(", ")})"
  }

  /**
    * Rules are injected per class, so they appear both at the top level and, for a
    * nested config class, under its own definition -- each alongside the properties it
    * constrains. Collect them wherever they sit.
    */
  private def rulesIn(node: JsonNode, schemaPath: String): Seq[Rule] = {
    if (!node.isObject) return Seq.empty

    val here = if (node.has(RulesKeyword)) {
      val rules = node.path(RulesKeyword)
      val properties = node.path("properties").fieldNames().asScala.toSet
      rules.fieldNames().asScala.toSeq.map { key =>
        Rule(schemaPath, key, rules.path(key), properties)
      }
    } else Seq.empty

    here ++ node
      .fields()
      .asScala
      .toSeq
      .flatMap(child => rulesIn(child.getValue, s"$schemaPath.${child.getKey}"))
  }

  private val rulesByOperator: Seq[(String, Seq[Rule])] =
    OperatorMetadataGenerator.operatorTypeMap.keys.toSeq
      .map(opClass =>
        opClass.getSimpleName -> rulesIn(
          OperatorMetadataGenerator.generateOperatorJsonSchema(opClass),
          "$"
        )
      )
      .filter { case (_, rules) => rules.nonEmpty }
      .sortBy { case (operator, _) => operator }

  // Without this the per-operator cases below would all vanish, and the suite would
  // pass by testing nothing.
  "Some operator" should s"declare $RulesKeyword, so the cases below are not vacuous" in {
    rulesByOperator should not be empty
  }

  rulesByOperator.foreach {
    case (operator, rules) =>
      behavior of s"The $RulesKeyword on $operator"

      it should "name only properties the generated schema declares" in {
        val unmatched = rules.filterNot(rule => rule.declaredProperties.contains(rule.key))
        withClue("rules naming no property: ")(unmatched shouldBe empty)
      }

      it should "state each rule as an object, so the constraint is readable" in {
        val notObjects = rules.filterNot(_.value.isObject)
        withClue("rules that are not objects: ")(notObjects shouldBe empty)
      }
  }
}
