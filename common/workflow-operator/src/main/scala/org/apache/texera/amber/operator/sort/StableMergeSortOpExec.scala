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

package org.apache.texera.amber.operator.sort

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.mutable.ArrayBuffer

/**
  * Stable in-memory merge sort for a single input partition.
  *
  * Strategy:
  *  - Buffer incoming tuples as size-1 sorted buckets.
  *  - Maintain a stack of buckets where adjacent buckets never share the same length.
  *  - On each push, perform "binary-carry" merges while the top two buckets have equal sizes.
  *  - At finish, collapse the stack left-to-right. Merging is stable (left wins on ties).
  *
  * Null policy:
  *  - Nulls are always ordered last, regardless of ascending/descending per key.
  */
class StableMergeSortOpExec(descString: String) extends OperatorExecutor {

  private val desc: StableMergeSortOpDesc =
    objectMapper.readValue(descString, classOf[StableMergeSortOpDesc])

  private var inputSchema: Schema = _

  /** Sort key resolved against the schema (index, data type, and direction). */
  private case class CompiledSortKey(
      index: Int,
      attributeType: AttributeType,
      descending: Boolean
  )

  /** Lexicographic sort keys compiled once on first tuple. */
  private var compiledSortKeys: Array[CompiledSortKey] = _

  /** Stack of sorted buckets. Invariant: no two adjacent buckets have equal lengths. */
  private var sortedBuckets: ArrayBuffer[ArrayBuffer[Tuple]] = _

  /** Exposed for testing: current bucket sizes from bottom to top of the stack. */
  private[sort] def debugBucketSizes: List[Int] =
    if (sortedBuckets == null) Nil else sortedBuckets.filter(_ != null).map(_.size).toList

  /** Initialize internal state. */
  override def open(): Unit = {
    sortedBuckets = ArrayBuffer.empty[ArrayBuffer[Tuple]]
  }

  /** Release internal buffers. */
  override def close(): Unit = {
    if (sortedBuckets != null) sortedBuckets.clear()
  }

  /**
    * Ingest a tuple. Defers emission until onFinish.
    *
    * Schema compilation happens on the first tuple.
    * Each tuple forms a size-1 sorted bucket that is pushed and possibly merged.
    */
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    if (inputSchema == null) {
      inputSchema = tuple.getSchema
      compiledSortKeys = compileSortKeys(inputSchema)
    }
    val sizeOneBucket = ArrayBuffer[Tuple](tuple)
    pushBucketAndCombine(sizeOneBucket)
    Iterator.empty
  }

  /**
    * Emit all sorted tuples by collapsing the bucket stack left-to-right.
    * Stability is preserved because merge prefers the left bucket on equality.
    */
  override def onFinish(port: Int): Iterator[TupleLike] = {
    if (sortedBuckets.isEmpty) return Iterator.empty

    var accumulator = sortedBuckets(0)
    var bucketIdx = 1
    while (bucketIdx < sortedBuckets.length) {
      accumulator = mergeSortedBuckets(accumulator, sortedBuckets(bucketIdx))
      bucketIdx += 1
    }

    sortedBuckets.clear()
    sortedBuckets.append(accumulator)
    accumulator.iterator
  }

  /**
    * Resolve logical sort keys to schema indices and attribute types.
    * Outputs an array of compiled sort keys used by [[compareBySortKeys]].
    */
  private def compileSortKeys(schema: Schema): Array[CompiledSortKey] = {
    desc.keys.map { sortCriteria: SortCriteriaUnit =>
      val name = sortCriteria.attributeName
      val index = schema.getIndex(name)
      val dataType = schema.getAttribute(name).getType
      val isDescending = sortCriteria.sortPreference == SortPreference.DESC
      CompiledSortKey(index, dataType, isDescending)
    }.toArray
  }

  /**
    * Push an already-sorted bucket and perform "binary-carry" merges while the
    * top two buckets have equal sizes.
    *
    * Scope:
    *  - Internal helper. Called by [[processTuple]] for size-1 buckets;
    *
    * Expected output:
    *  - Updates the internal stack so that no two adjacent buckets have equal sizes.
    *
    * Limitations / possible issues:
    *  - The given bucket must already be sorted by [[compareBySortKeys]].
    *  - Stability relies on left-before-right merge order; do not reorder parameters.
    *
    * Complexity:
    *  - Amortized O(1) per push; total O(n log n) over n tuples.
    */
  private[sort] def pushBucketAndCombine(newBucket: ArrayBuffer[Tuple]): Unit = {
    sortedBuckets.append(newBucket)
    // Merge while top two buckets are equal-sized; left-before-right preserves stability.
    while (
      sortedBuckets.length >= 2 &&
      sortedBuckets(sortedBuckets.length - 1).size == sortedBuckets(sortedBuckets.length - 2).size
    ) {
      val right = sortedBuckets.remove(sortedBuckets.length - 1) // newer
      val left = sortedBuckets.remove(sortedBuckets.length - 1) // older
      val merged = mergeSortedBuckets(left, right)
      sortedBuckets.append(merged)
    }
  }

  /**
    * Stable two-way merge of two buckets already sorted by [[compareBySortKeys]].
    *
    * Scope:
    *  - Internal helper used during incremental carries and final collapse.
    *
    * Expected output:
    *  - A new bucket with all elements of both inputs, globally sorted.
    *
    * Limitations / possible issues:
    *  - Both inputs must be sorted with the same key config; behavior is undefined otherwise.
    *  - Stability guarantee: if keys are equal, the element from the left bucket is emitted first.
    *
    * Complexity:
    *  - O(left.size + right.size)
    */
  private[sort] def mergeSortedBuckets(
      leftBucket: ArrayBuffer[Tuple],
      rightBucket: ArrayBuffer[Tuple]
  ): ArrayBuffer[Tuple] = {
    val outMerged = new ArrayBuffer[Tuple](leftBucket.size + rightBucket.size)
    var leftIndex = 0
    var rightIndex = 0
    while (leftIndex < leftBucket.size && rightIndex < rightBucket.size) {
      if (compareBySortKeys(leftBucket(leftIndex), rightBucket(rightIndex)) <= 0) {
        outMerged += leftBucket(leftIndex); leftIndex += 1
      } else {
        outMerged += rightBucket(rightIndex); rightIndex += 1
      }
    }
    while (leftIndex < leftBucket.size) { outMerged += leftBucket(leftIndex); leftIndex += 1 }
    while (rightIndex < rightBucket.size) { outMerged += rightBucket(rightIndex); rightIndex += 1 }
    outMerged
  }

  /**
    * Lexicographic comparison of two tuples using the compiled sort keys.
    *
    * Semantics:
    *  - Nulls are always ordered last, regardless of sort direction.
    *  - For non-null values, comparison is type-aware (see [[compareTypedNonNullValues]]).
    *  - If a key compares equal, evaluation proceeds to the next key.
    *  - Descending reverses the sign of the base comparison.
    *
    * Limitations / possible issues:
    *  - Requires [[compiledSortKeys]] to be initialized; called after the first tuple.
    *  - For unsupported types, [[compareTypedNonNullValues]] throws IllegalStateException.
    */
  private def compareBySortKeys(left: Tuple, right: Tuple): Int = {
    var keyIndex = 0
    while (keyIndex < compiledSortKeys.length) {
      val currentKey = compiledSortKeys(keyIndex)
      val leftValue = left.getField[Any](currentKey.index)
      val rightValue = right.getField[Any](currentKey.index)

      // Null policy: ALWAYS last, regardless of ASC/DESC
      if (leftValue == null || rightValue == null) {
        if (leftValue == null && rightValue == null) {
          keyIndex += 1
        } else {
          return if (leftValue == null) 1 else -1
        }
      } else {
        val base = compareTypedNonNullValues(leftValue, rightValue, currentKey.attributeType)
        if (base != 0) return if (currentKey.descending) -base else base
        keyIndex += 1
      }
    }
    0
  }

  /**
    * Compare two non-null values using their attribute type.
    *
    * Type semantics:
    *  - INTEGER, LONG: numeric ascending via Java primitive compares.
    *  - DOUBLE: java.lang.Double.compare (orders -Inf < ... < +Inf < NaN).
    *  - BOOLEAN: false < true.
    *  - TIMESTAMP: java.sql.Timestamp#compareTo.
    *  - STRING: String#compareTo (UTF-16, lexicographic).
    *  - BINARY: unsigned lexicographic order over byte arrays:
    *        - Compare byte-by-byte treating each as 0..255 (mask 0xff).
    *        - The first differing byte decides the order.
    *        - If all compared bytes are equal, the shorter array sorts first.
    *        - Example: [] < [0x00] < [0x00,0x00] < [0x00,0x01] < [0x7F] < [0x80] < [0xFF].
    */
  private def compareTypedNonNullValues(
      leftValue: Any,
      rightValue: Any,
      attrType: AttributeType
  ): Int =
    attrType match {
      case AttributeType.INTEGER =>
        java.lang.Integer.compare(
          leftValue.asInstanceOf[Number].intValue(),
          rightValue.asInstanceOf[Number].intValue()
        )
      case AttributeType.LONG =>
        java.lang.Long.compare(
          leftValue.asInstanceOf[Number].longValue(),
          rightValue.asInstanceOf[Number].longValue()
        )
      case AttributeType.DOUBLE =>
        java.lang.Double.compare(
          leftValue.asInstanceOf[Number].doubleValue(),
          rightValue.asInstanceOf[Number].doubleValue()
        )
      case AttributeType.BOOLEAN =>
        java.lang.Boolean.compare(leftValue.asInstanceOf[Boolean], rightValue.asInstanceOf[Boolean])
      case AttributeType.TIMESTAMP =>
        leftValue
          .asInstanceOf[java.sql.Timestamp]
          .compareTo(rightValue.asInstanceOf[java.sql.Timestamp])
      case AttributeType.STRING =>
        leftValue.asInstanceOf[String].compareTo(rightValue.asInstanceOf[String])
      case AttributeType.BINARY =>
        java.util.Arrays.compareUnsigned(
          leftValue.asInstanceOf[Array[Byte]],
          rightValue.asInstanceOf[Array[Byte]]
        )
      case other =>
        throw new IllegalStateException(s"Unsupported attribute type $other in StableMergeSort")
    }
}
