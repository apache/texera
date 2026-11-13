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
import org.apache.texera.amber.operator.hashJoin.HashJoinOpDesc.HASH_JOIN_INTERNAL_KEY_NAME
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class HashJoinProbeOpExecSpec extends AnyFlatSpec {
  private val buildPort: Int = 0
  private val probePort: Int = 1

  private def schema(name: String): Schema = {
    Schema()
      .add(new Attribute(name, AttributeType.STRING))
      .add(new Attribute(name + "_1", AttributeType.STRING))
  }

  private def internalHashTableSchema: Schema = {
    Schema()
      .add(HASH_JOIN_INTERNAL_KEY_NAME, AttributeType.STRING)
      .add(schema("build"))
  }

  private def buildTuple(payload: String, key: String): Tuple = {
    Tuple.builder(schema("build")).addSequentially(Array[Any](payload, key)).build()
  }

  private def probeTuple(payload: String, key: String): Tuple = {
    Tuple.builder(schema("probe")).addSequentially(Array[Any](payload, key)).build()
  }

  private def descFor(joinType: JoinType): HashJoinOpDesc[String] = {
    val desc = new HashJoinOpDesc[String]()
    desc.buildAttributeName = "build_1"
    desc.probeAttributeName = "probe_1"
    desc.joinType = joinType
    desc
  }

  /**
    * Runs the build side through HashJoinBuildOpExec and feeds its output into
    * the probe executor's port 0, mirroring how the two executors are chained
    * in a real workflow.
    */
  private def probeExecWithBuildSide(
      joinType: JoinType,
      buildTuples: Seq[Tuple]
  ): HashJoinProbeOpExec[String] = {
    val descString = objectMapper.writeValueAsString(descFor(joinType))

    val buildExec = new HashJoinBuildOpExec[String](descString)
    buildExec.open()
    buildTuples.foreach(t => assert(buildExec.processTuple(t, buildPort).isEmpty))

    val probeExec = new HashJoinProbeOpExec[String](descString)
    probeExec.open()
    buildExec
      .onFinish(buildPort)
      .foreach(tupleLike =>
        assert(
          probeExec
            .processTuple(
              tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(internalHashTableSchema),
              buildPort
            )
            .isEmpty
        )
      )
    buildExec.close()

    assert(probeExec.onFinish(buildPort).isEmpty)
    probeExec
  }

  private def defaultBuildSide: Seq[Tuple] = (0 to 7).map(i => buildTuple(s"b$i", i.toString))

  private def fieldMappingsOf(outputs: Iterator[TupleLike]): List[Map[String, Any]] = {
    outputs.map(_.asInstanceOf[MapTupleLike].fieldMappings).toList
  }

  "HashJoinProbeOpExec" should "emit only joined tuples for INNER join" in {
    val probeExec = probeExecWithBuildSide(JoinType.INNER, defaultBuildSide)

    val outputs = (5 to 9)
      .flatMap(i =>
        fieldMappingsOf(probeExec.processTuple(probeTuple(s"p$i", i.toString), probePort))
      )

    assert(outputs.size == 3)
    assert(outputs.map(_("build_1")).toSet == Set("5", "6", "7"))
    // The probe key column is dropped from the joined tuple; the remaining
    // probe payload column is carried over next to the build columns.
    outputs.foreach(m => assert(m.keySet == Set("build", "build_1", "probe")))

    assert(probeExec.onFinish(probePort).isEmpty)
    probeExec.close()
  }

  it should "not emit unmatched probe tuples but emit unmatched build tuples for LEFT_OUTER join" in {
    val probeExec = probeExecWithBuildSide(JoinType.LEFT_OUTER, defaultBuildSide)

    // Unmatched probe tuples must produce nothing under LEFT_OUTER.
    (8 to 9).foreach(i =>
      assert(probeExec.processTuple(probeTuple(s"p$i", i.toString), probePort).isEmpty)
    )
    // Matched probe tuples still join as usual.
    (5 to 7).foreach(i =>
      assert(probeExec.processTuple(probeTuple(s"p$i", i.toString), probePort).size == 1)
    )

    // Only never-matched build tuples come out, exactly once each, in no
    // guaranteed order.
    val unmatched = fieldMappingsOf(probeExec.onFinish(probePort))
    assert(unmatched.size == 5)
    assert(unmatched.map(_("build_1")).toSet == Set("0", "1", "2", "3", "4"))
    unmatched.foreach(m => assert(m.keySet == Set("build", "build_1")))

    probeExec.close()
  }

  it should "emit unmatched probe tuples but not unmatched build tuples for RIGHT_OUTER join" in {
    val probeExec = probeExecWithBuildSide(JoinType.RIGHT_OUTER, defaultBuildSide)

    // Unmatched probe tuples pass through unchanged under RIGHT_OUTER.
    val unmatched = (8 to 9)
      .flatMap(i =>
        fieldMappingsOf(probeExec.processTuple(probeTuple(s"p$i", i.toString), probePort))
      )
    assert(unmatched.size == 2)
    assert(unmatched.map(_("probe_1")).toSet == Set("8", "9"))
    unmatched.foreach(m => assert(m.keySet == Set("probe", "probe_1")))

    // Matched probe tuples still join as usual.
    (5 to 7).foreach(i =>
      assert(probeExec.processTuple(probeTuple(s"p$i", i.toString), probePort).size == 1)
    )

    // Build-side leftovers must not be emitted under RIGHT_OUTER.
    assert(probeExec.onFinish(probePort).isEmpty)
    probeExec.close()
  }

  it should "emit both unmatched probe and unmatched build tuples for FULL_OUTER join" in {
    val probeExec = probeExecWithBuildSide(JoinType.FULL_OUTER, defaultBuildSide)

    val probeOutputs = (5 to 9)
      .flatMap(i =>
        fieldMappingsOf(probeExec.processTuple(probeTuple(s"p$i", i.toString), probePort))
      )
    // 3 joined tuples (keys 5-7) plus 2 unmatched probe tuples (keys 8-9).
    assert(probeOutputs.size == 5)
    assert(probeOutputs.count(_.keySet == Set("build", "build_1", "probe")) == 3)
    assert(probeOutputs.count(_.keySet == Set("probe", "probe_1")) == 2)

    val unmatchedBuild = fieldMappingsOf(probeExec.onFinish(probePort))
    assert(unmatchedBuild.size == 5)
    assert(unmatchedBuild.map(_("build_1")).toSet == Set("0", "1", "2", "3", "4"))

    probeExec.close()
  }

  it should "not re-emit a matched build key in the LEFT_OUTER anti-join" in {
    val probeExec = probeExecWithBuildSide(JoinType.LEFT_OUTER, defaultBuildSide)

    // Probe the same key twice; the joined flag must stay flipped.
    assert(probeExec.processTuple(probeTuple("p5", "5"), probePort).size == 1)
    assert(probeExec.processTuple(probeTuple("p5x", "5"), probePort).size == 1)

    val unmatched = fieldMappingsOf(probeExec.onFinish(probePort))
    assert(unmatched.size == 7)
    assert(!unmatched.map(_("build_1")).contains("5"))
    assert(unmatched.map(_("build_1")).toSet == Set("0", "1", "2", "3", "4", "6", "7"))

    probeExec.close()
  }

  it should "emit one joined tuple per build tuple sharing the probed key" in {
    val buildSide = Seq(
      buildTuple("first", "dup"),
      buildTuple("second", "dup"),
      buildTuple("other", "solo")
    )
    val probeExec = probeExecWithBuildSide(JoinType.INNER, buildSide)

    val outputs = fieldMappingsOf(probeExec.processTuple(probeTuple("p", "dup"), probePort))
    assert(outputs.size == 2)
    assert(outputs.map(_("build")).toSet == Set("first", "second"))
    outputs.foreach(m => assert(m("probe") == "p"))

    probeExec.close()
  }

  it should "handle an empty build side according to join type" in {
    val innerExec = probeExecWithBuildSide(JoinType.INNER, Seq.empty)
    assert(innerExec.processTuple(probeTuple("p", "k"), probePort).isEmpty)
    assert(innerExec.onFinish(probePort).isEmpty)
    innerExec.close()

    val fullOuterExec = probeExecWithBuildSide(JoinType.FULL_OUTER, Seq.empty)
    // Every probe tuple passes through unchanged, and there is nothing to
    // anti-join on the build side.
    val outputs = fieldMappingsOf(fullOuterExec.processTuple(probeTuple("p", "k"), probePort))
    assert(outputs.size == 1)
    assert(outputs.head.keySet == Set("probe", "probe_1"))
    assert(fullOuterExec.onFinish(probePort).isEmpty)
    fullOuterExec.close()
  }

  it should "emit every build tuple, including all rows of a shared key, when no probe tuple arrives" in {
    val buildSide = Seq(
      buildTuple("first", "dup"),
      buildTuple("second", "dup"),
      buildTuple("other", "solo")
    )
    val probeExec = probeExecWithBuildSide(JoinType.LEFT_OUTER, buildSide)

    val unmatched = fieldMappingsOf(probeExec.onFinish(probePort))
    assert(unmatched.size == 3)
    assert(unmatched.map(_("build")).toSet == Set("first", "second", "other"))

    probeExec.close()
  }

  it should "join tuples whose keys are both null (current behavior)" in {
    // Unlike SQL join semantics, where NULL never equals NULL, the hash map
    // treats null keys as equal, so null-keyed tuples join with each other.
    val probeExec =
      probeExecWithBuildSide(JoinType.INNER, Seq(buildTuple("b", null), buildTuple("b0", "0")))

    val outputs = fieldMappingsOf(probeExec.processTuple(probeTuple("p", null), probePort))
    assert(outputs.size == 1)
    assert(outputs.head("build") == "b")
    assert(outputs.head("probe") == "p")

    probeExec.close()
  }

  it should "store build tuples without the internal hash key and clear the map on close" in {
    val probeExec = probeExecWithBuildSide(JoinType.INNER, defaultBuildSide)

    assert(probeExec.buildTableHashMap.size == 8)
    probeExec.buildTableHashMap.values.foreach {
      case (tuples, joined) =>
        assert(!joined)
        tuples.foreach(t =>
          assert(!t.getSchema.getAttributeNames.contains(HASH_JOIN_INTERNAL_KEY_NAME))
        )
    }

    // A successful probe flips only that key's joined flag.
    assert(probeExec.processTuple(probeTuple("p5", "5"), probePort).size == 1)
    assert(probeExec.buildTableHashMap("5")._2)
    assert(probeExec.buildTableHashMap.filter(_._1 != "5").values.forall(!_._2))

    probeExec.close()
    assert(probeExec.buildTableHashMap.isEmpty)
  }
}
