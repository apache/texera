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

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

/**
  * A serializable description of how a [[PhysicalOp]] derives its output partition
  * from its input partitions.
  *
  * The runtime previously stored this as a function closure
  * (`List[PartitionInfo] => PartitionInfo`) directly on `PhysicalOp`, which made the
  * `PhysicalOp` impossible to serialize to JSON. This ADT captures everything the
  * function needs as plain, Jackson-serializable data; the actual function is rebuilt
  * lazily via [[toFunction]] after deserialization, reproducing the original behavior
  * exactly.
  */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  Array(
    new Type(value = classOf[Passthrough], name = "passthrough"),
    new Type(value = classOf[ToSingle], name = "toSingle"),
    new Type(value = classOf[ToHash], name = "toHash"),
    new Type(value = classOf[ToUnknown], name = "toUnknown"),
    new Type(value = classOf[ProjectionPartition], name = "projection")
  )
)
sealed trait DerivePartitionSpec {

  /**
    * Rebuilds the partition-derivation function described by this spec.
    */
  def toFunction: List[PartitionInfo] => PartitionInfo
}

/**
  * Default behavior: the output partition is the same as the (first) input partition.
  * Matches the historical default closure `inputParts => inputParts.head`.
  */
final case class Passthrough() extends DerivePartitionSpec {
  override def toFunction: List[PartitionInfo] => PartitionInfo = inputParts => inputParts.head
}

/**
  * Always produces a [[SinglePartition]] regardless of the inputs.
  * Used by `manyToOnePhysicalOp`.
  */
final case class ToSingle() extends DerivePartitionSpec {
  override def toFunction: List[PartitionInfo] => PartitionInfo = _ => SinglePartition()
}

/**
  * Always produces a [[HashPartition]] on the given attribute names (empty means all
  * attributes). Used by Aggregate (group-by keys), HashJoin (probe attribute),
  * Intersect, Distinct, Difference, and SymmetricDifference.
  */
final case class ToHash(hashAttributeNames: List[String] = List.empty) extends DerivePartitionSpec {
  override def toFunction: List[PartitionInfo] => PartitionInfo =
    _ => HashPartition(hashAttributeNames)
}

/**
  * Always produces an [[UnknownPartition]] regardless of the inputs.
  * Used by Python / Java / R UDF operators.
  */
final case class ToUnknown() extends DerivePartitionSpec {
  override def toFunction: List[PartitionInfo] => PartitionInfo = _ => UnknownPartition()
}

/**
  * Reproduces the partition-derivation logic of `ProjectionOpDesc`.
  *
  * The original closure inspects only the incoming partition and re-emits it,
  * collapsing a hash/range partition to [[UnknownPartition]] when its attribute-name
  * list is empty. It does not depend on any operator-specific descriptor state, so the
  * spec carries no fields and reproduces the function exactly.
  */
final case class ProjectionPartition() extends DerivePartitionSpec {
  override def toFunction: List[PartitionInfo] => PartitionInfo =
    partition => {
      val inputPartitionInfo = partition.head
      inputPartitionInfo match {
        case HashPartition(hashAttributeNames) =>
          if (hashAttributeNames.nonEmpty) HashPartition(hashAttributeNames)
          else UnknownPartition()
        case RangePartition(rangeAttributeNames, min, max) =>
          if (rangeAttributeNames.nonEmpty) RangePartition(rangeAttributeNames, min, max)
          else UnknownPartition()
        case _ => inputPartitionInfo
      }
    }
}
