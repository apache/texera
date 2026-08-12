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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}

import scala.jdk.CollectionConverters._

/**
  * The shared numeric dataset for the Sklearn operator families, a checked-in
  * JSON resource (mirrors [[CanonicalFixture]]) so the table is human-readable
  * and lives in one place. A small, well-separated 2-feature binary-classification
  * table: 6 rows per class — enough members for cv=5 estimators
  * (LogisticRegressionCV, probability calibration). Numeric-only because sklearn
  * cannot fit the canonical auto-fixture's string columns.
  *
  * Source of truth: src/test/resources/verify/sklearn_fixture.json. `schema`
  * below stays authoritative for column types (JSON has no typed columns).
  */
object SklearnFixture extends SharedFixture {

  val schema: Schema = new Schema(
    new Attribute("x1", AttributeType.DOUBLE),
    new Attribute("x2", AttributeType.DOUBLE),
    new Attribute("y", AttributeType.INTEGER)
  )

  /** Both ports get the whole table: the classifier operators train on port 0 and
    * test on port 1, and the point of the pair is the two ports, not two datasets.
    */
  override def rowsFor(port: Int): Seq[Tuple] = rows

  /** `y` keeps every value: it is the label the estimator fits against, so a hole
    * in it asks what sklearn does with an unlabelled row rather than what the
    * operator does with a missing feature.
    */
  override val keepFilled: Set[String] = Set("y")

  private val fixtureResource = "/verify/sklearn_fixture.json"

  val rows: Vector[Tuple] = {
    val stream = Option(getClass.getResourceAsStream(fixtureResource))
      .getOrElse(sys.error(s"sklearn fixture not found on classpath: $fixtureResource"))
    val root =
      try new ObjectMapper().readTree(stream)
      finally stream.close()
    root
      .elements()
      .asScala
      .map { node =>
        val b = Tuple.builder(schema)
        schema.getAttributes.foreach { attr =>
          val cell = node.get(attr.getName)
          require(cell != null, s"sklearn fixture row missing column '${attr.getName}'")
          val value: AnyRef = attr.getType match {
            case AttributeType.INTEGER => Int.box(cell.asInt())
            case AttributeType.DOUBLE  => Double.box(cell.asDouble())
            case _                     => cell.asText()
          }
          b.add(attr, value)
        }
        b.build()
      }
      .toVector
  }
}

/**
  * Text dataset for the sklearn `countVectorizer=true` path. Two token-disjoint
  * classes so `CountVectorizer` + any estimator separates them perfectly and
  * both paths predict identically (deterministic parity). Mirrors
  * [[SklearnFixture]]'s 6-rows-per-class size so cv=5 estimators
  * (LogisticRegressionCV, probability calibration) have enough members. `note`
  * is column 0 so the model probe — which feeds a text pipeline the probe's
  * first column as a Series — picks it up as the vectorized feature.
  *
  * Source of truth: src/test/resources/verify/sklearn_text_fixture.json (mirrors
  * [[SklearnFixture]]); `schema` below stays authoritative for column types.
  */
object SklearnTextFixture extends SharedFixture {

  val schema: Schema = new Schema(
    new Attribute("note", AttributeType.STRING),
    new Attribute("y", AttributeType.INTEGER)
  )

  /** Both ports get the whole table, as in [[SklearnFixture]]. */
  override def rowsFor(port: Int): Seq[Tuple] = rows

  /** `y` is the label, for the reason [[SklearnFixture.keepFilled]] gives. */
  override val keepFilled: Set[String] = Set("y")

  private val fixtureResource = "/verify/sklearn_text_fixture.json"

  val rows: Vector[Tuple] = {
    val stream = Option(getClass.getResourceAsStream(fixtureResource))
      .getOrElse(sys.error(s"sklearn text fixture not found on classpath: $fixtureResource"))
    val root =
      try new ObjectMapper().readTree(stream)
      finally stream.close()
    root
      .elements()
      .asScala
      .map { node =>
        val b = Tuple.builder(schema)
        schema.getAttributes.foreach { attr =>
          val cell = node.get(attr.getName)
          require(cell != null, s"sklearn text fixture row missing column '${attr.getName}'")
          val value: AnyRef = attr.getType match {
            case AttributeType.INTEGER => Int.box(cell.asInt())
            case _                     => cell.asText()
          }
          b.add(attr, value)
        }
        b.build()
      }
      .toVector
  }
}
