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

package org.apache.texera.amber.core.workflow

import com.fasterxml.jackson.annotation.JsonSubTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionInfoSpec extends AnyFlatSpec with Matchers {

  // ----- satisfies -----

  "satisfies" should "report a partition as satisfying itself" in {
    HashPartition(List("k")).satisfies(HashPartition(List("k"))) shouldBe true
    SinglePartition().satisfies(SinglePartition()) shouldBe true
    BroadcastPartition().satisfies(BroadcastPartition()) shouldBe true
  }

  it should "treat UnknownPartition as the universal accepter (any partition satisfies Unknown)" in {
    // The convention is that "no partition requirement" is satisfied by
    // every concrete partition.
    HashPartition(List("k")).satisfies(UnknownPartition()) shouldBe true
    SinglePartition().satisfies(UnknownPartition()) shouldBe true
    BroadcastPartition().satisfies(UnknownPartition()) shouldBe true
    OneToOnePartition().satisfies(UnknownPartition()) shouldBe true
  }

  it should "not relax the requirement: UnknownPartition does NOT satisfy a concrete partition" in {
    UnknownPartition().satisfies(SinglePartition()) shouldBe false
    UnknownPartition().satisfies(HashPartition()) shouldBe false
  }

  it should "report different concrete partitions as not satisfying each other" in {
    HashPartition(List("k")).satisfies(SinglePartition()) shouldBe false
    SinglePartition().satisfies(HashPartition(List("k"))) shouldBe false
    HashPartition(List("a")).satisfies(HashPartition(List("b"))) shouldBe false
  }

  // ----- merge -----

  "merge" should "preserve the partition when merging with itself" in {
    val hp = HashPartition(List("k"))
    hp.merge(hp) shouldBe hp
    SinglePartition().merge(SinglePartition()) shouldBe SinglePartition()
    BroadcastPartition().merge(BroadcastPartition()) shouldBe BroadcastPartition()
  }

  it should "fall back to UnknownPartition when merging two different concrete partitions" in {
    HashPartition(List("k")).merge(SinglePartition()) shouldBe UnknownPartition()
    SinglePartition().merge(BroadcastPartition()) shouldBe UnknownPartition()
  }

  it should "always return UnknownPartition when merging RangePartition with anything (override)" in {
    // RangePartition.merge overrides the default to always return Unknown,
    // because two range-partitioned streams merged without a re-sort lose
    // their ordering invariant.
    val rp = RangePartition(List("k"), 0L, 100L)
    rp.merge(rp) shouldBe UnknownPartition()
    rp.merge(HashPartition()) shouldBe UnknownPartition()
    rp.merge(UnknownPartition()) shouldBe UnknownPartition()
  }

  it should "merge anything with UnknownPartition into UnknownPartition" in {
    HashPartition(List("k")).merge(UnknownPartition()) shouldBe UnknownPartition()
    SinglePartition().merge(UnknownPartition()) shouldBe UnknownPartition()
  }

  // ----- RangePartition.apply factory -----

  "RangePartition.apply" should "return a RangePartition when range attribute names are non-empty" in {
    RangePartition(List("k"), 0L, 100L) shouldBe a[RangePartition]
  }

  it should "fall back to UnknownPartition when the range attribute name list is empty" in {
    // Pin: an empty rangeAttributeNames list collapses to UnknownPartition
    // — without this fallback the subsequent shuffle code would have nothing
    // to range-partition on.
    RangePartition(List.empty, 0L, 100L) shouldBe UnknownPartition()
  }

  // ----- HashPartition default attribute list -----

  "HashPartition()" should "default to an empty hash attribute list" in {
    HashPartition().hashAttributeNames shouldBe empty
  }

  // ----- JsonSubTypes registration -----

  "PartitionInfo @JsonSubTypes" should "list the current registration set (omits OneToOnePartition)" in {
    // Pin: the @JsonSubTypes annotation on PartitionInfo currently registers
    // HashPartition, RangePartition, SinglePartition, BroadcastPartition,
    // and UnknownPartition — but NOT OneToOnePartition. The "all" claim was
    // moved to the pendingUntilFixed test below so this spec only documents
    // the present-day set.
    val annotation = classOf[PartitionInfo].getAnnotation(classOf[JsonSubTypes])
    val registered = annotation.value().toList.map(_.value().getSimpleName).toSet
    registered shouldBe Set(
      "HashPartition",
      "RangePartition",
      "SinglePartition",
      "BroadcastPartition",
      "UnknownPartition"
    )
  }

  it should "eventually register every concrete PartitionInfo subclass (pendingUntilFixed)" in pendingUntilFixed {
    // Intended contract: every concrete PartitionInfo subtype must be
    // reachable through the polymorphic dispatch on `type`, otherwise
    // Jackson cannot deserialize the missing payload (today: OneToOne-
    // Partition). Asserting `contains "OneToOnePartition"` here flips this
    // test from Pending to a real pass once the bug is fixed — pendingUntil-
    // Fixed inverts that and turns the now-passing assertion into a failure
    // so the fix has to delete the marker deliberately.
    val annotation = classOf[PartitionInfo].getAnnotation(classOf[JsonSubTypes])
    val registered = annotation.value().toList.map(_.value().getSimpleName).toSet
    registered should contain("OneToOnePartition")
  }

  // ----- case class equality -----

  "PartitionInfo case classes" should "use structural equality (case-class semantics)" in {
    HashPartition(List("k")) shouldBe HashPartition(List("k"))
    HashPartition(List("k")) should not be HashPartition(List("other"))
    SinglePartition() shouldBe SinglePartition()
    UnknownPartition() shouldBe UnknownPartition()
  }
}
