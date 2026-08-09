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

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper
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
    "register every concrete PartitionInfo subclass" in {
    val annotation = classOf[PartitionInfo].getAnnotation(classOf[JsonSubTypes])
    val registered = annotation.value().toList.map(_.value().getSimpleName).toSet
    assert(
      registered == Set(
        "HashPartition",
        "RangePartition",
        "SinglePartition",
        "OneToOnePartition",
        "BroadcastPartition",
        "SignalPartition",
        "UnknownPartition"
      )
    )
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

  // ---------------------------------------------------------------------------
  // Attribute-list sensitivity of the HashPartition algebra
  // ---------------------------------------------------------------------------

  "HashPartition" should "treat the empty attribute list as its own partitioning" in {
    val hashAll: PartitionInfo = HashPartition()
    // hash-on-all-attributes is not interchangeable with hash-on-"a"
    assert(!hashAll.satisfies(hashA))
    assert(!hashA.satisfies(hashAll))
    assert(hashAll.merge(hashA) == unknown)
    assert(hashA.merge(hashAll) == unknown)
    // but it still merges with itself to itself, and satisfies Unknown
    assert(hashAll.merge(HashPartition()) == hashAll)
    assert(hashAll.satisfies(unknown))
  }

  it should "be order-sensitive in its hash attribute list" in {
    val ab: PartitionInfo = HashPartition(List("a", "b"))
    val ba: PartitionInfo = HashPartition(List("b", "a"))
    assert(ab != ba)
    assert(!ab.satisfies(ba))
    assert(ab.merge(ba) == unknown)
    assert(ab.merge(HashPartition(List("a", "b"))) == ab)
  }

  // ---------------------------------------------------------------------------
  // Range bounds participate in identity, but never in merge
  // ---------------------------------------------------------------------------

  "RangePartition" should "distinguish two ranges that differ only in their bounds" in {
    val narrow: PartitionInfo = new RangePartition(List("a"), 0L, 10L)
    val wide: PartitionInfo = new RangePartition(List("a"), 0L, 20L)
    assert(narrow != wide)
    assert(!narrow.satisfies(wide))
    // identical bounds are the same partition, so satisfies holds ...
    assert(narrow.satisfies(new RangePartition(List("a"), 0L, 10L)))
    // ... yet merging still forfeits the sort order (override always returns Unknown)
    assert(narrow.merge(new RangePartition(List("a"), 0L, 10L)) == unknown)
    assert(narrow.merge(wide) == unknown)
  }

  it should "keep whatever bounds it is given, including inverted and negative ones" in {
    // the factory only inspects the attribute list; the bounds are stored verbatim
    val inverted = RangePartition(List("a"), 10L, -5L).asInstanceOf[RangePartition]
    assert(inverted.rangeMin == 10L)
    assert(inverted.rangeMax == -5L)
    val extremes = RangePartition(List("a", "b"), Long.MinValue, Long.MaxValue)
      .asInstanceOf[RangePartition]
    assert(extremes.rangeAttributeNames == List("a", "b"))
    assert(extremes.rangeMin == Long.MinValue)
    assert(extremes.rangeMax == Long.MaxValue)
  }

  it should "collapse to UnknownPartition on an empty attribute list regardless of bounds" in {
    // the empty-attribute short-circuit ignores the bounds entirely
    assert(RangePartition(List.empty, Long.MinValue, Long.MaxValue) == UnknownPartition())
    assert(RangePartition(List.empty, 5L, 5L) == UnknownPartition())
  }

  // ---------------------------------------------------------------------------
  // Polymorphic JSON serialization (the @JsonTypeInfo / @JsonSubTypes contract)
  // ---------------------------------------------------------------------------

  private val jsonCases: List[(String, PartitionInfo)] = List(
    "hash" -> HashPartition(List("a", "b")),
    "hash" -> HashPartition(),
    "range" -> new RangePartition(List("a"), -1L, 42L),
    "single" -> SinglePartition(),
    "oneToOne" -> OneToOnePartition(),
    "broadcast" -> BroadcastPartition(),
    "signal" -> SignalPartition(),
    "none" -> UnknownPartition()
  )

  "PartitionInfo JSON" should "tag every subtype with its registered type name" in {
    jsonCases.foreach {
      case (typeName, partition) =>
        val node = objectMapper.readTree(objectMapper.writeValueAsString(partition))
        assert(node.has("type"), s"$partition must carry a type discriminator")
        assert(node.get("type").asText() == typeName, s"$partition must be tagged '$typeName'")
    }
  }

  it should "round-trip every subtype back to an equal value through the base type" in {
    jsonCases.foreach {
      case (_, partition) =>
        val restored =
          objectMapper.readValue(objectMapper.writeValueAsString(partition), classOf[PartitionInfo])
        assert(restored == partition, s"$partition must survive a JSON round-trip")
        assert(restored.getClass == partition.getClass)
    }
  }

  it should "preserve the range bounds and attribute names of a RangePartition" in {
    val original = new RangePartition(List("a", "b"), -7L, 99L)
    val restored = objectMapper
      .readValue(objectMapper.writeValueAsString(original), classOf[PartitionInfo])
      .asInstanceOf[RangePartition]
    assert(restored.rangeAttributeNames == List("a", "b"))
    assert(restored.rangeMin == -7L)
    assert(restored.rangeMax == 99L)
  }

  "PartitionInfo @JsonTypeInfo" should "use a NAME discriminator on a 'type' property" in {
    val annotation = classOf[PartitionInfo].getAnnotation(classOf[JsonTypeInfo])
    assert(annotation.use() == JsonTypeInfo.Id.NAME)
    assert(annotation.include() == JsonTypeInfo.As.PROPERTY)
    assert(annotation.property() == "type")
  }
}
