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

import org.apache.texera.amber.core.workflow.PortIdentity
import org.scalatest.flatspec.AnyFlatSpec

class TupleLikeSpec extends AnyFlatSpec {

  private val intAttr = new Attribute("col-int", AttributeType.INTEGER)
  private val strAttr = new Attribute("col-string", AttributeType.STRING)
  private val schema = Schema().add(intAttr).add(strAttr)

  // ----- internal markers (case-class synthetics; construction covered by InternalMarkerSpec) -----

  "FinalizePort" should "support copy, hashCode, toString, and product members" in {
    val marker = FinalizePort(PortIdentity(1), input = true)
    assert(marker.copy(input = false) == FinalizePort(PortIdentity(1), input = false))
    assert(marker.hashCode == FinalizePort(PortIdentity(1), input = true).hashCode)
    assert(marker.toString.startsWith("FinalizePort("))
    assert(marker.productArity == 2)
    assert(marker.productElement(0) == PortIdentity(1))
    assert(marker.productElement(1) == true)
    assert(marker.productPrefix == "FinalizePort")
    assert(!marker.canEqual(FinalizeExecutor()))
  }

  "FinalizeExecutor" should "support copy, hashCode, toString, and product members" in {
    val marker = FinalizeExecutor()
    assert(marker.copy() == marker)
    assert(marker.hashCode == FinalizeExecutor().hashCode)
    assert(marker.toString == "FinalizeExecutor()")
    assert(marker.productArity == 0)
  }

  // ----- SeqTupleLike -----

  "SeqTupleLike" should "not implement inMemSize" in {
    assertThrows[NotImplementedError](TupleLike(1, 2).inMemSize)
  }

  it should "enforce a schema by pairing fields positionally" in {
    val tuple = TupleLike(42, "hello").enforceSchema(schema)
    assert(tuple.getField[Int]("col-int") == 42)
    assert(tuple.getField[String]("col-string") == "hello")
  }

  it should "fail schema enforcement when fields are missing or extra" in {
    assertThrows[TupleBuildingException](TupleLike(42).enforceSchema(schema))
    assertThrows[IndexOutOfBoundsException](TupleLike(42, "hello", true).enforceSchema(schema))
  }

  // ----- MapTupleLike -----

  "MapTupleLike" should "expose its mapping values as fields and not implement inMemSize" in {
    val mapLike = TupleLike(Map[String, Any]("a" -> 1, "b" -> "x"))
    assert(mapLike.getFields.toSet == Set[Any](1, "x"))
    assertThrows[NotImplementedError](mapLike.inMemSize)
  }

  it should "enforce a schema by attribute name, nulling missing keys and dropping extras" in {
    val tuple =
      TupleLike(Map[String, Any]("col-int" -> 7, "unrelated-key" -> true)).enforceSchema(schema)
    assert(tuple.getField[Int]("col-int") == 7)
    assert(tuple.getField[Any]("col-string") == null)
  }

  // ----- TupleLike factory overloads -----

  "TupleLike" should "build from a Map of field mappings" in {
    val mapLike = TupleLike(Map[String, Any]("k1" -> 1, "k2" -> "v"))
    assert(mapLike.fieldMappings == Map[String, Any]("k1" -> 1, "k2" -> "v"))
  }

  it should "build from an Iterable of name-value pairs" in {
    val pairs: Iterable[(String, Any)] = Seq("k1" -> 1, "k2" -> "v")
    val mapLike = TupleLike(pairs)
    assert(mapLike.fieldMappings == Map[String, Any]("k1" -> 1, "k2" -> "v"))
  }

  it should "build from name-value pair varargs with last-wins duplicate keys" in {
    val mapLike = TupleLike("k1" -> 1, "k2" -> "v")
    assert(mapLike.fieldMappings == Map[String, Any]("k1" -> 1, "k2" -> "v"))
    assert(TupleLike("k" -> 1, "k" -> 2).fieldMappings == Map[String, Any]("k" -> 2))
  }

  it should "build from a java.util.List of fields" in {
    val javaList = new java.util.ArrayList[Any]()
    javaList.add(1)
    javaList.add("x")
    assert(TupleLike(javaList).getFields.toSeq == Seq(1, "x"))
  }

  it should "build from field varargs for non-iterable types" in {
    assert(TupleLike(1, 2, 3).getFields.toSeq == Seq(1, 2, 3))
    assert(TupleLike("a", "b").getFields.toSeq == Seq("a", "b"))
    assert(TupleLike(1, "a").getFields.toSeq == Seq(1, "a"))
  }

  it should "provide no NotAnIterable evidence for iterable types" in {
    // The guard implicit exists so iterable varargs cannot materialize
    // evidence; invoking it directly pins the rejection.
    val ex = intercept[RuntimeException](TupleLike.NotAnIterable.iterableIsNotAnIterable[List, Any])
    assert(ex.getMessage == "Iterable types are not allowed")
  }

  it should "build from a single Iterable of fields" in {
    assert(TupleLike(List(1, 2, 3)).getFields.toSeq == Seq(1, 2, 3))
  }

  it should "build from an Array of fields, keeping the array by reference" in {
    val array: Array[Any] = Array(1, "x")
    assert(TupleLike(array).getFields eq array)
  }
}
