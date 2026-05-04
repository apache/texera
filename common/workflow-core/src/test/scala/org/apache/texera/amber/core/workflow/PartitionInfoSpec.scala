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

class PartitionInfoSpec extends AnyFlatSpec {

  // The full set of "named" partition kinds we care about cross-checking.
  // Two HashPartitions with different attribute lists count as a "different
  // partition" too, so we include both shapes.
  private val hashA: PartitionInfo = HashPartition(List("a"))
  private val hashB: PartitionInfo = HashPartition(List("b"))
  private val rangeA: PartitionInfo = new RangePartition(List("a"), 0L, 10L)
  private val single: PartitionInfo = SinglePartition()
  private val broadcast: PartitionInfo = BroadcastPartition()
  private val oneToOne: PartitionInfo = OneToOnePartition()
  private val unknown: PartitionInfo = UnknownPartition()

  // Five "primary" partition kinds (excluding the duplicate Hash and the
  // catch-all Unknown — both handled separately) used for the cross product.
  private val primaryKinds: List[(String, PartitionInfo)] = List(
    "HashPartition" -> hashA,
    "RangePartition" -> rangeA,
    "SinglePartition" -> single,
    "BroadcastPartition" -> broadcast,
    "OneToOnePartition" -> oneToOne
  )

  "PartitionInfo.satisfies" should "hold reflexively (each partition satisfies itself)" in {
    primaryKinds.foreach {
      case (name, p) =>
        assert(p.satisfies(p), s"$name should satisfy itself")
    }
    // UnknownPartition reflexively satisfies itself too.
    assert(unknown.satisfies(unknown))
    // HashPartition with the same attribute list satisfies itself even
    // across distinct instances.
    assert(HashPartition(List("a")).satisfies(HashPartition(List("a"))))
  }

  it should "fail across the full 5x5 cross-product of distinct primary kinds" in {
    // For every pair of distinct primary partition kinds, satisfies must be
    // false. This covers the full 5x5 = 25 cell matrix; the diagonal is
    // covered by the reflexivity test above.
    for {
      (lname, lhs) <- primaryKinds
      (rname, rhs) <- primaryKinds
      if lhs != rhs
    } {
      assert(!lhs.satisfies(rhs), s"$lname must not satisfy $rname")
    }
  }

  it should "hold for any primary partition against UnknownPartition" in {
    primaryKinds.foreach {
      case (name, p) =>
        assert(p.satisfies(unknown), s"$name should satisfy UnknownPartition")
    }
    // And UnknownPartition satisfies itself.
    assert(unknown.satisfies(unknown))
  }

  it should "fail when UnknownPartition is on the LHS against any primary kind" in {
    primaryKinds.foreach {
      case (name, p) =>
        assert(!unknown.satisfies(p), s"UnknownPartition must not satisfy $name")
    }
  }

  it should "fail for HashPartition with different attribute lists (and otherwise-equal shape)" in {
    assert(!hashA.satisfies(hashB))
    assert(!hashB.satisfies(hashA))
    // But both still satisfy UnknownPartition.
    assert(hashA.satisfies(unknown))
    assert(hashB.satisfies(unknown))
  }

  "PartitionInfo.merge" should "preserve the partition when merged with itself across every kind" in {
    primaryKinds.foreach {
      case (name, p) =>
        // RangePartition has its own override that always returns
        // UnknownPartition (covered separately below); skip it here.
        if (!p.isInstanceOf[RangePartition]) {
          assert(p.merge(p) == p, s"$name should merge with itself to itself")
        }
    }
    // UnknownPartition merges with itself to itself.
    assert(unknown.merge(unknown) == unknown)
    // HashPartition with same attributes merges to itself.
    assert(HashPartition(List("a")).merge(HashPartition(List("a"))) == HashPartition(List("a")))
  }

  it should "fall back to UnknownPartition for the full 5x5 cross-product of distinct primary kinds" in {
    // Every distinct-pair merge produces UnknownPartition.
    for {
      (lname, lhs) <- primaryKinds
      (rname, rhs) <- primaryKinds
      if lhs != rhs
    } {
      assert(
        lhs.merge(rhs) == unknown,
        s"$lname.merge($rname) must be UnknownPartition"
      )
    }
  }

  it should "fall back to UnknownPartition when either side is UnknownPartition (excluding self-merge)" in {
    primaryKinds.foreach {
      case (name, p) =>
        assert(p.merge(unknown) == unknown, s"$name.merge(Unknown) must be Unknown")
        assert(unknown.merge(p) == unknown, s"Unknown.merge($name) must be Unknown")
    }
  }

  it should "always return UnknownPartition for RangePartition merges, including with itself" in {
    val r = new RangePartition(List("a"), 0L, 10L)
    assert(r.merge(r) == unknown, "RangePartition self-merge is overridden to Unknown")
    primaryKinds.foreach {
      case (name, p) =>
        assert(r.merge(p) == unknown, s"RangePartition.merge($name) must be Unknown")
    }
  }

  it should "treat HashPartitions with different attribute lists as distinct (merge → Unknown)" in {
    assert(hashA.merge(hashB) == unknown)
    assert(hashB.merge(hashA) == unknown)
  }

  "RangePartition.apply" should "return an UnknownPartition when no range attributes are provided" in {
    assert(RangePartition(List.empty, 0L, 10L) == UnknownPartition())
  }

  it should "return a RangePartition when at least one range attribute is provided" in {
    val result = RangePartition(List("a"), 0L, 10L)
    assert(result.isInstanceOf[RangePartition])
    val rp = result.asInstanceOf[RangePartition]
    assert(rp.rangeAttributeNames == List("a"))
    assert(rp.rangeMin == 0L)
    assert(rp.rangeMax == 10L)
  }

  // ---------------------------------------------------------------------------
  // HashPartition default attribute list
  // ---------------------------------------------------------------------------

  "HashPartition()" should "default to an empty hash attribute list" in {
    assert(HashPartition().hashAttributeNames.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // JsonSubTypes registration
  // ---------------------------------------------------------------------------

  "PartitionInfo @JsonSubTypes" should
    "list the current registration set (omits OneToOnePartition)" in {
    // Pin: the @JsonSubTypes annotation on PartitionInfo currently registers
    // HashPartition, RangePartition, SinglePartition, BroadcastPartition,
    // and UnknownPartition — but NOT OneToOnePartition. The "all" claim is
    // documented separately in the pendingUntilFixed test below so this
    // spec only documents the present-day set.
    val annotation = classOf[PartitionInfo].getAnnotation(classOf[JsonSubTypes])
    val registered = annotation.value().toList.map(_.value().getSimpleName).toSet
    assert(
      registered == Set(
        "HashPartition",
        "RangePartition",
        "SinglePartition",
        "BroadcastPartition",
        "UnknownPartition"
      )
    )
  }

  it should "eventually register every concrete PartitionInfo subclass (pendingUntilFixed)" in pendingUntilFixed {
    // Intended contract: every concrete PartitionInfo subtype must be
    // reachable through the polymorphic dispatch on `type`, otherwise
    // Jackson cannot deserialize the missing payload (today: OneToOne-
    // Partition). Asserting `contains "OneToOnePartition"` here flips
    // this test from Pending to a real pass once the bug is fixed —
    // pendingUntilFixed inverts that and turns the now-passing
    // assertion into a failure so the fix has to delete the marker
    // deliberately.
    val annotation = classOf[PartitionInfo].getAnnotation(classOf[JsonSubTypes])
    val registered = annotation.value().toList.map(_.value().getSimpleName).toSet
    assert(registered.contains("OneToOnePartition"))
  }

  // ---------------------------------------------------------------------------
  // case-class equality
  // ---------------------------------------------------------------------------

  "PartitionInfo case classes" should "use structural equality (case-class semantics)" in {
    assert(HashPartition(List("k")) == HashPartition(List("k")))
    assert(HashPartition(List("k")) != HashPartition(List("other")))
    assert(SinglePartition() == SinglePartition())
    assert(UnknownPartition() == UnknownPartition())
  }
}
