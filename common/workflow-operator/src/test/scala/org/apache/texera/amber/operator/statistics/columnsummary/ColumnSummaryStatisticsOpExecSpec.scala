/*

* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
  */

package org.apache.texera.amber.operator.statistics.columnsummary

import org.apache.texera.amber.core.tuple.{
  Attribute,
  AttributeType,
  Schema,
  SchemaEnforceable,
  Tuple,
  TupleLike}
import org.scalatest.funsuite.AnyFunSuite

class ColumnSummaryStatisticsOpExecSpec extends AnyFunSuite {

  private def makeSchema(fields: (String, AttributeType)*): Schema =
    Schema(fields.map { case (name, attributeType) =>
      new Attribute(name, attributeType)
    }.toList)

  private def makeTuple(schema: Schema, values: Any*): Tuple =
    Tuple(schema, values.toArray)

  private val outputSchema: Schema =
    Schema(
      List(
        new Attribute("columnName", AttributeType.STRING),
        new Attribute("dataType", AttributeType.STRING),
        new Attribute("rowCount", AttributeType.INTEGER),
        new Attribute("nullCount", AttributeType.INTEGER),
        new Attribute("nonNullCount", AttributeType.INTEGER),
        new Attribute("minValue", AttributeType.STRING),
        new Attribute("maxValue", AttributeType.STRING),
        new Attribute("meanValue", AttributeType.DOUBLE)
      )
    )

  private def executeOperator(tuples: Seq[Tuple]): List[TupleLike] = {
    val exec = new ColumnSummaryStatisticsOpExec("")
    exec.open()
    tuples.foreach(tuple => exec.processTuple(tuple, 0))
    val results = exec.onFinish(0).toList
    exec.close()
    results
  }

  private def rowsByColumn(results: List[TupleLike]): Map[String, Tuple] =
    results
      .map(_.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
      .map(row => row.getField[String]("columnName") -> row)
      .toMap

  test("computes min, max, mean, and null counts for an integer column") {
    val schema = makeSchema("score" -> AttributeType.INTEGER)

    val results = executeOperator(
      Seq(
        makeTuple(schema, 10),
        makeTuple(schema, null),
        makeTuple(schema, 30)
      )
    )

    assert(results.size == 1)

    val score = rowsByColumn(results)("score")

    assert(score.getField[String]("columnName") == "score")
    assert(score.getField[String]("dataType") == AttributeType.INTEGER.name())
    assert(score.getField[Int]("rowCount") == 3)
    assert(score.getField[Int]("nullCount") == 1)
    assert(score.getField[Int]("nonNullCount") == 2)
    assert(score.getField[String]("minValue") == "10.0")
    assert(score.getField[String]("maxValue") == "30.0")
    assert(math.abs(score.getField[Double]("meanValue") - 20.0) < 1e-6)

  }

  test("computes numeric statistics while leaving non-numeric statistics null") {
    val schema = makeSchema(
      "price" -> AttributeType.DOUBLE,
      "category" -> AttributeType.STRING
    )

    val results = executeOperator(
      Seq(
        makeTuple(schema, 1.5, "book"),
        makeTuple(schema, 2.5, null),
        makeTuple(schema, null, "game")
      )
    )

    assert(results.size == 2)

    val rows = rowsByColumn(results)
    val price = rows("price")
    val category = rows("category")

    assert(price.getField[String]("dataType") == AttributeType.DOUBLE.name())
    assert(price.getField[Int]("rowCount") == 3)
    assert(price.getField[Int]("nullCount") == 1)
    assert(price.getField[Int]("nonNullCount") == 2)
    assert(price.getField[String]("minValue") == "1.5")
    assert(price.getField[String]("maxValue") == "2.5")
    assert(math.abs(price.getField[Double]("meanValue") - 2.0) < 1e-6)

    assert(category.getField[String]("dataType") == AttributeType.STRING.name())
    assert(category.getField[Int]("rowCount") == 3)
    assert(category.getField[Int]("nullCount") == 1)
    assert(category.getField[Int]("nonNullCount") == 2)
    assert(category.getField[String]("minValue") == null)
    assert(category.getField[String]("maxValue") == null)
    assert(category.getField[Any]("meanValue") == null)
  }

  test("returns one summary row for each input column") {
    val schema = makeSchema(
      "id" -> AttributeType.INTEGER,
      "name" -> AttributeType.STRING,
      "amount" -> AttributeType.DOUBLE
    )

    val results = executeOperator(
      Seq(
        makeTuple(schema, 1, "alpha", 10.0),
        makeTuple(schema, 2, "beta", 20.0)
      )
    )

    val rows = rowsByColumn(results)

    assert(results.size == 3)
    assert(rows.keySet == Set("id", "name", "amount"))

    assert(rows("id").getField[Int]("rowCount") == 2)
    assert(rows("name").getField[Int]("rowCount") == 2)
    assert(rows("amount").getField[Int]("rowCount") == 2)

  }

  test("returns no rows when no input tuples are processed") {
    val results = executeOperator(Seq.empty)

    assert(results.isEmpty)

  }
}
