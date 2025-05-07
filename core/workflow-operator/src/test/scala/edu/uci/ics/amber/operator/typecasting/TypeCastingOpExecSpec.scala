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

package edu.uci.ics.amber.operator.typecasting

import edu.uci.ics.amber.core.tuple._
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class TypeCastingOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.BOOLEAN))
    .add(new Attribute("field4", AttributeType.LONG))

  val castToSchema: Schema = Schema()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.STRING))
    .add(new Attribute("field3", AttributeType.STRING))
    .add(new Attribute("field4", AttributeType.LONG))

  val castingUnit1 = new TypeCastingUnit()
  castingUnit1.attribute = "field2"
  castingUnit1.resultType = AttributeType.STRING
  val castingUnit2 = new TypeCastingUnit()
  castingUnit2.attribute = "field3"
  castingUnit2.resultType = AttributeType.STRING
  val castingUnits: List[TypeCastingUnit] = List(castingUnit1, castingUnit2)

  val opDesc: TypeCastingOpDesc = new TypeCastingOpDesc()
  opDesc.typeCastingUnits = castingUnits
  val tuple: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "hello")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(
      new Attribute("field3", AttributeType.BOOLEAN),
      true
    )
    .add(
      new Attribute("field4", AttributeType.LONG),
      3L
    )
    .build()

  it should "open" in {

    val typeCastingOpExec = new TypeCastingOpExec(objectMapper.writeValueAsString(opDesc))
    typeCastingOpExec.open()

  }

  it should "process Tuple" in {

    val typeCastingOpExec = new TypeCastingOpExec(objectMapper.writeValueAsString(opDesc))

    typeCastingOpExec.open()

    val outputTuple =
      typeCastingOpExec
        .processTuple(tuple, 0)
        .next()
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(castToSchema)

    assert(outputTuple.length == 4)
    assert(outputTuple.getField("field1").asInstanceOf[String] == "hello")
    assert(outputTuple.getField("field2").asInstanceOf[String] == "1")
    assert(outputTuple.getField("field3").asInstanceOf[String] == "true")
    assert(outputTuple.getField("field4").asInstanceOf[Long] == 3L)
    assert("hello" == outputTuple.getField[String](0))
    assert(outputTuple.getField[String](1) == "1")
    assert(outputTuple.getField[String](2) == "true")
    assert(outputTuple.getField[Long](3) == 3L)
  }
}
