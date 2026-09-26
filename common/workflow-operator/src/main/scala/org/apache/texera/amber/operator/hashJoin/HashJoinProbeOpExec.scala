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

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.operator.hashJoin.HashJoinOpDesc.HASH_JOIN_INTERNAL_KEY_NAME
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object JoinUtils {

  /** The name a right column takes in the joined row: "#@1" appended until it is
    * free of every left name and of every other right name. The schema the
    * operator declares, a matched row and an unmatched right row all name their
    * right columns by this one rule, since the row is matched to the schema by
    * name and a column the two name apart loses its value.
    */
  def renamed(name: String, leftNames: Seq[String], rightNames: Seq[String]): String = {
    val others = rightNames.filterNot(_ == name)
    var newName = name
    while (leftNames.contains(newName) || others.contains(newName)) newName = s"$newName#@1"
    newName
  }

  def joinTuples(
      leftTuple: Tuple,
      rightTuple: Tuple,
      skipAttributeName: Option[String] = None
  ): TupleLike = {
    val leftAttributeNames = leftTuple.getSchema.getAttributeNames
    val rightAttributeNames = rightTuple.getSchema.getAttributeNames.filterNot(name =>
      skipAttributeName.isDefined && name == skipAttributeName.get
    )
    // Create a Map from leftTuple's fields
    val leftTupleFields: Map[String, Any] = leftAttributeNames
      .map(name => name -> leftTuple.getField(name))
      .toMap

    // Create a Map from rightTuple's fields, renaming conflicts
    val rightTupleFields = rightAttributeNames
      .map { name =>
        renamed(name, leftAttributeNames, rightAttributeNames) -> rightTuple.getField[Any](name)
      }

    TupleLike(leftTupleFields ++ rightTupleFields)
  }
}

class HashJoinProbeOpExec[K](
    descString: String
) extends OperatorExecutor {

  private val desc: HashJoinOpDesc[K] =
    objectMapper.readValue(descString, classOf[HashJoinOpDesc[K]])
  var buildTableHashMap: mutable.HashMap[K, (ListBuffer[Tuple], Boolean)] = _

  // The build side's column names, learned from its first row. An unmatched
  // right row has no build row of its own to name them.
  private var leftAttributeNames: List[String] = List.empty

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, (mutable.ListBuffer[Tuple], Boolean)]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
    if (port == 0) {
      // Load build hash map
      val key = tuple.getField[K](HASH_JOIN_INTERNAL_KEY_NAME)
      val names = tuple.getSchema.getAttributeNames.filterNot(n => n == HASH_JOIN_INTERNAL_KEY_NAME)
      if (leftAttributeNames.isEmpty) leftAttributeNames = names
      buildTableHashMap.getOrElseUpdate(key, (new ListBuffer[Tuple](), false))._1 += tuple
        .getPartialTuple(names)
      Iterator.empty
    } else {
      // Probe phase
      val key = tuple.getField(desc.probeAttributeName).asInstanceOf[K]
      val (matchedTuples, joined) =
        buildTableHashMap.getOrElse(key, (new ListBuffer[Tuple](), false))

      if (matchedTuples.nonEmpty) {
        // Join match found
        buildTableHashMap.put(key, (matchedTuples, true))
        performJoin(tuple, matchedTuples)
      } else if (desc.joinType == JoinType.RIGHT_OUTER || desc.joinType == JoinType.FULL_OUTER) {
        // Handle right and full outer joins without a match
        performRightAntiJoin(tuple)
      } else {
        // No match found
        Iterator.empty
      }
    }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    if (
      port == 1 && (desc.joinType == JoinType.LEFT_OUTER || desc.joinType == JoinType.FULL_OUTER)
    ) {
      // Handle left and full outer joins after input is exhausted
      performLeftAntiJoin
    } else {
      Iterator.empty
    }

  }

  private def performLeftAntiJoin: Iterator[TupleLike] = {
    buildTableHashMap.valuesIterator
      .collect { case (tuples: ListBuffer[Tuple], joined: Boolean) if !joined => tuples }
      .flatMap { tuples =>
        tuples.map { tuple =>
          TupleLike(
            tuple.getSchema.getAttributeNames
              .map(attributeName => attributeName -> tuple.getField(attributeName)): _*
          )
        }
      }
  }

  private def performJoin(
      probeTuple: Tuple,
      matchedTuples: ListBuffer[Tuple]
  ): Iterator[TupleLike] = {
    matchedTuples.iterator.map { buildTuple =>
      JoinUtils.joinTuples(
        buildTuple,
        probeTuple,
        skipAttributeName = Some(desc.probeAttributeName)
      )
    }
  }

  // The right columns are named as a matched row names them, so a column the
  // left side also names goes under its suffixed name and not into the left
  // column. The probe key keeps its own name: where both sides name the key
  // alike, the unmatched row's key is what fills the left key column.
  private def performRightAntiJoin(tuple: Tuple): Iterator[TupleLike] = {
    val rightNames = tuple.getSchema.getAttributeNames.filterNot(_ == desc.probeAttributeName)
    Iterator(
      TupleLike(
        tuple.getSchema.getAttributeNames.map { name =>
          val named =
            if (name == desc.probeAttributeName) name
            else JoinUtils.renamed(name, leftAttributeNames, rightNames)
          named -> tuple.getField[Any](name)
        }: _*
      )
    )
  }
}
