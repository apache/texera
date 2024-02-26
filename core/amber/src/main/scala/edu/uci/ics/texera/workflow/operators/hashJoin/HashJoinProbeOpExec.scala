package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala

class HashJoinProbeOpExec[K](
    buildAttributeName: String,
    probeAttributeName: String,
    joinType: JoinType
) extends OperatorExecutor {
  var currentTuple: Tuple = _

  var buildTableHashMap: mutable.HashMap[K, (ListBuffer[Tuple], Boolean)] = _

  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[TupleLike] =
    tuple match {
      case Left(tuple) if port == 0 =>
        // Load build hash map
        buildTableHashMap(tuple.getField("key")) = (tuple.getField("value"), false)
        Iterator.empty

      case Left(tuple) =>
        // Probe phase
        val key = tuple.getField(probeAttributeName).asInstanceOf[K]
        val (matchedTuples, joined) =
          buildTableHashMap.getOrElse(key, (new ListBuffer[Tuple](), false))

        if (matchedTuples.nonEmpty) {
          // Join match found
          buildTableHashMap.put(key, (matchedTuples, true))
          performJoin(tuple, matchedTuples)
        } else if (joinType == JoinType.RIGHT_OUTER || joinType == JoinType.FULL_OUTER) {
          // Handle right and full outer joins without a match
          performRightAntiJoin(tuple)
        } else {
          // No match found
          Iterator.empty
        }

      case Right(_)
          if port != 0 && (joinType == JoinType.LEFT_OUTER || joinType == JoinType.FULL_OUTER) =>
        // Handle left and full outer joins after input is exhausted
        performLeftAntiJoin

      case _ =>
        // Default case for all other conditions
        Iterator.empty
    }

  private def performLeftAntiJoin: Iterator[TupleLike] = {
    buildTableHashMap.valuesIterator
      .collect { case (tuples: ListBuffer[Tuple], joined: Boolean) if !joined => tuples }
      .flatMap { tuples =>
        tuples.map { tuple =>
          TupleLike(
            tuple.getSchema.getAttributeNames.asScala
              .map(attributeName => attributeName -> tuple.getField(attributeName))
              .toSeq: _*
          )
        }
      }
  }

  private def performJoin(
      probeTuple: Tuple,
      matchedTuples: ListBuffer[Tuple]
  ): Iterator[TupleLike] = {
    matchedTuples.iterator.map { buildTuple =>
      // Create a Map from buildTuple's attributes
      val buildTupleAttributes: Map[String, Any] = buildTuple.getSchema.getAttributeNames.asScala
        .map(name => name -> buildTuple.getField(name))
        .toMap

      // Create a Map from probeTuple's attributes, renaming conflicts
      val probeTupleAttributes = probeTuple.getSchema.getAttributeNames.asScala
        .map { name =>
          val newName =
            if (name != buildAttributeName && buildTupleAttributes.contains(name)) s"$name#@1"
            else name
          newName -> probeTuple.getField[Any](name)
        }

      TupleLike((buildTupleAttributes ++ probeTupleAttributes).toSeq: _*)
    }
  }

  private def performRightAntiJoin(tuple: Tuple): Iterator[TupleLike] =
    Iterator(
      TupleLike(
        tuple.getSchema.getAttributeNames.asScala
          .map(attributeName => attributeName -> tuple.getField(attributeName))
          .toSeq: _*
      )
    )

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, (mutable.ListBuffer[Tuple], Boolean)]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }

}
