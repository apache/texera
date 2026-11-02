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

import org.apache.texera.amber.core.tuple._
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.hashJoin.HashJoinOpDesc.HASH_JOIN_INTERNAL_KEY_NAME
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class HashJoinOpSpec extends AnyFlatSpec {
  val build: Int = 0
  val probe: Int = 1

  case class JoinResult(
      tuples: List[TupleLike],
      onFinishTuples: List[TupleLike]
  )

  def getInternalHashTableSchema(buildInputSchema: Schema): Schema = {
    Schema()
      .add(HASH_JOIN_INTERNAL_KEY_NAME, AttributeType.ANY)
      .add(buildInputSchema)
  }

  def tuple(name: String, n: Int = 1, i: Option[Int]): Tuple = {
    Tuple
      .builder(schema(name, n))
      .addSequentially(Array[Any](i.map(_.toString).orNull, i.map(_.toString).orNull))
      .build()
  }

  def schema(name: String, n: Int = 1): Schema = {
    Schema()
      .add(new Attribute(name, AttributeType.STRING))
      .add(new Attribute(name + "_" + n, AttributeType.STRING))
  }

  private def runJoin(
      opDesc: HashJoinOpDesc[String],
      inputSchemas: Map[PortIdentity, Schema],
      buildTuples: Seq[Tuple],
      probeTuples: Seq[Tuple]
  ): JoinResult = {
    val outputSchema = opDesc.getExternalOutputSchemas(inputSchemas).values.head
    val descJson = objectMapper.writeValueAsString(opDesc)

    val buildOpExec = new HashJoinBuildOpExec[String](descJson)
    buildOpExec.open()
    try {
      buildTuples.foreach { t =>
        assert(buildOpExec.processTuple(t, build).isEmpty)
      }
      val buildOpOutputIterator = buildOpExec.onFinish(build)
      assert(buildOpOutputIterator.hasNext)

      val probeOpExec = new HashJoinProbeOpExec[String](descJson)
      probeOpExec.open()
      try {
        while (buildOpOutputIterator.hasNext) {
          assert(
            probeOpExec
              .processTuple(
                buildOpOutputIterator
                  .next()
                  .asInstanceOf[SchemaEnforceable]
                  .enforceSchema(getInternalHashTableSchema(inputSchemas.head._2)),
                build
              )
              .isEmpty
          )
        }
        assert(probeOpExec.onFinish(build).isEmpty)

        val outputTuples = probeTuples
          .map(t => probeOpExec.processTuple(t, probe))
          .foldLeft(Iterator[TupleLike]())(_ ++ _)
          .map(_.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
          .toList

        val onFinishTuples = probeOpExec
          .onFinish(probe)
          .map(_.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
          .toList

        JoinResult(outputTuples, onFinishTuples)
      } finally {
        probeOpExec.close()
      }
    } finally {
      buildOpExec.close()
    }
  }

  it should "work with basic two input streams with different buildAttributeName and probeAttributeName" in {
    val opDesc = new HashJoinOpDesc[String]()
    opDesc.buildAttributeName = "build_1"
    opDesc.probeAttributeName = "probe_1"
    opDesc.joinType = JoinType.INNER
    val inputSchemas = Map(PortIdentity() -> schema("build"), PortIdentity(1) -> schema("probe"))

    val result = runJoin(
      opDesc,
      inputSchemas,
      buildTuples = (0 to 7).map(i => tuple("build", 1, Some(i))),
      probeTuples = (5 to 9).map(i => tuple("probe", 1, Some(i)))
    )

    assert(result.onFinishTuples.isEmpty)
    assert(result.tuples.size == 3)
    assert(result.tuples.head.getFields.length == 3)
  }

  it should "work with basic two input streams with the same buildAttributeName and probeAttributeName" in {
    val opDesc = new HashJoinOpDesc[String]()
    opDesc.buildAttributeName = "same"
    opDesc.probeAttributeName = "same"
    opDesc.joinType = JoinType.INNER
    val inputSchemas =
      Map(PortIdentity() -> schema("same", 1), PortIdentity(1) -> schema("same", 2))

    val result = runJoin(
      opDesc,
      inputSchemas,
      buildTuples = (0 to 7).map(i => tuple("same", 1, Some(i))),
      probeTuples = (5 to 9).map(i => tuple("same", n = 2, Some(i)))
    )

    assert(result.onFinishTuples.isEmpty)
    assert(result.tuples.size == 3)
    assert(result.tuples.head.getFields.length == 3)
  }

  it should "work with basic two input streams with the same buildAttributeName and probeAttributeName with Full Outer Join" in {
    val opDesc = new HashJoinOpDesc[String]()
    opDesc.buildAttributeName = "same"
    opDesc.probeAttributeName = "same"
    opDesc.joinType = JoinType.FULL_OUTER
    val inputSchemas =
      Map(PortIdentity() -> schema("same", 1), PortIdentity(1) -> schema("same", 2))

    val result = runJoin(
      opDesc,
      inputSchemas,
      buildTuples = (0 to 7).map(i => tuple("same", 1, Some(i))),
      probeTuples = (5 to 9).map(_ => tuple("same", n = 2, None))
    )

    assert(result.tuples.size == 5)
    assert(result.onFinishTuples.size == 8)
  }
}
