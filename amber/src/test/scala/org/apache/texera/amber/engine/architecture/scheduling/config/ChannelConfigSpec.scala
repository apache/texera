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

package org.apache.texera.amber.engine.architecture.scheduling.config

import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.core.workflow.{
  BroadcastPartition,
  HashPartition,
  OneToOnePartition,
  PortIdentity,
  RangePartition,
  SinglePartition,
  UnknownPartition
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ChannelConfigSpec extends AnyFlatSpec with Matchers {

  private val port: PortIdentity = PortIdentity(id = 0, internal = false)

  private def actor(name: String): ActorVirtualIdentity = ActorVirtualIdentity(name)

  private val w1 = actor("w1")
  private val w2 = actor("w2")
  private val w3 = actor("w3")
  private val u1 = actor("u1")
  private val u2 = actor("u2")
  private val u3 = actor("u3")

  // Helper: extract the (sender, receiver) endpoint pairs from a list of
  // ChannelConfigs to make the assertions readable.
  private def endpoints(cs: List[ChannelConfig]): List[(String, String)] =
    cs.map(c => (c.channelId.fromWorkerId.name, c.channelId.toWorkerId.name))

  // ----- cross-product partition arms -----

  "generateChannelConfigs" should "produce the full from*to cross product for HashPartition" in {
    val out = ChannelConfig.generateChannelConfigs(
      List(w1, w2),
      List(u1, u2, u3),
      port,
      HashPartition()
    )
    endpoints(out) shouldBe List(
      ("w1", "u1"),
      ("w1", "u2"),
      ("w1", "u3"),
      ("w2", "u1"),
      ("w2", "u2"),
      ("w2", "u3")
    )
    out.foreach(_.channelId.isControl shouldBe false)
    out.foreach(_.toPortId shouldBe port)
  }

  it should "produce the full from*to cross product for BroadcastPartition" in {
    val out = ChannelConfig.generateChannelConfigs(
      List(w1, w2),
      List(u1, u2),
      port,
      BroadcastPartition()
    )
    endpoints(out) shouldBe List(("w1", "u1"), ("w1", "u2"), ("w2", "u1"), ("w2", "u2"))
  }

  it should "produce the full from*to cross product for RangePartition" in {
    val out = ChannelConfig.generateChannelConfigs(
      List(w1),
      List(u1, u2),
      port,
      RangePartition(List("k"), 0L, 100L)
    )
    endpoints(out) shouldBe List(("w1", "u1"), ("w1", "u2"))
  }

  it should "produce the full from*to cross product for UnknownPartition" in {
    val out = ChannelConfig.generateChannelConfigs(
      List(w1, w2),
      List(u1),
      port,
      UnknownPartition()
    )
    endpoints(out) shouldBe List(("w1", "u1"), ("w2", "u1"))
  }

  // ----- SinglePartition arm -----

  "SinglePartition" should "produce one channel per from-worker to the single to-worker" in {
    val out = ChannelConfig.generateChannelConfigs(
      List(w1, w2, w3),
      List(u1),
      port,
      SinglePartition()
    )
    endpoints(out) shouldBe List(("w1", "u1"), ("w2", "u1"), ("w3", "u1"))
  }

  it should "raise an AssertionError when more than one to-worker is supplied" in {
    // Pin: SinglePartition is only valid when collapsing onto exactly one
    // downstream worker; passing more violates the assertion in the source.
    assertThrows[AssertionError] {
      ChannelConfig.generateChannelConfigs(
        List(w1, w2),
        List(u1, u2),
        port,
        SinglePartition()
      )
    }
  }

  // ----- OneToOnePartition arm -----

  "OneToOnePartition" should "zip equal-length from and to lists pairwise" in {
    val out = ChannelConfig.generateChannelConfigs(
      List(w1, w2, w3),
      List(u1, u2, u3),
      port,
      OneToOnePartition()
    )
    endpoints(out) shouldBe List(("w1", "u1"), ("w2", "u2"), ("w3", "u3"))
  }

  it should "truncate to the shorter list when from and to lengths differ (current behavior)" in {
    // Pin: Scala List.zip drops the tail of the longer side. Callers are
    // expected to enforce equal lengths upstream; an asymmetric input here
    // silently loses pairings rather than raising. Documenting so a future
    // tightening (e.g. require/asserting equal lengths) breaks this spec
    // on purpose and forces the contract change to be reviewed.
    val out = ChannelConfig.generateChannelConfigs(
      List(w1, w2, w3),
      List(u1, u2),
      port,
      OneToOnePartition()
    )
    endpoints(out) shouldBe List(("w1", "u1"), ("w2", "u2"))

    val out2 = ChannelConfig.generateChannelConfigs(
      List(w1),
      List(u1, u2, u3),
      port,
      OneToOnePartition()
    )
    endpoints(out2) shouldBe List(("w1", "u1"))
  }

  // ----- empty inputs -----

  // The previous block ended with `"OneToOnePartition" should ...`, so switch
  // back to `generateChannelConfigs` here. Otherwise the empty-input cases
  // (which exercise Hash/Broadcast arms too) and the toPortId test below
  // would be reported as `"OneToOnePartition" should ...`.
  "generateChannelConfigs" should "return an empty list when fromWorkerIds is empty (cross-product arm)" in {
    val out = ChannelConfig.generateChannelConfigs(
      Nil,
      List(u1, u2),
      port,
      HashPartition()
    )
    out shouldBe empty
  }

  it should "return an empty list when toWorkerIds is empty (cross-product arm)" in {
    val out = ChannelConfig.generateChannelConfigs(
      List(w1, w2),
      Nil,
      port,
      HashPartition()
    )
    out shouldBe empty
  }

  it should "return an empty list when both inputs are empty (OneToOne)" in {
    val out = ChannelConfig.generateChannelConfigs(
      Nil,
      Nil,
      port,
      OneToOnePartition()
    )
    out shouldBe empty
  }

  // ----- toPortId propagation -----

  it should "propagate the same toPortId onto every produced ChannelConfig" in {
    val customPort = PortIdentity(id = 7, internal = true)
    val out = ChannelConfig.generateChannelConfigs(
      List(w1, w2),
      List(u1, u2),
      customPort,
      BroadcastPartition()
    )
    out.foreach(_.toPortId shouldBe customPort)
  }
}
