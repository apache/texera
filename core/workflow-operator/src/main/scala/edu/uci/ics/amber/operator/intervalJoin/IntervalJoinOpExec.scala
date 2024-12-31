package edu.uci.ics.amber.operator.intervalJoin

import edu.uci.ics.amber.core.WorkflowRuntimeException
import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{AttributeType, Tuple, TupleLike}
import edu.uci.ics.amber.operator.hashJoin.JoinUtils
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

/** This Operator have two assumptions:
  * 1. The tuples in both inputs come in ascending order
  * 2. The left input join key takes as points, join condition is: left key in the range of (right key, right key + constant)
  */
class IntervalJoinOpExec(
    descString: String
) extends OperatorExecutor {
  private val desc: IntervalJoinOpDesc =
    objectMapper.readValue(descString, classOf[IntervalJoinOpDesc])
  var leftTable: ListBuffer[Tuple] = new ListBuffer[Tuple]()
  var rightTable: ListBuffer[Tuple] = new ListBuffer[Tuple]()

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    if (port == 0) {
      leftTable += tuple
      if (rightTable.nonEmpty) {
        removeTooSmallTupleInRightCache(leftTable.head)
        rightTable
          .filter(rightTableTuple => {

            intervalCompare(
              tuple.getField(desc.leftAttributeName),
              rightTableTuple.getField(desc.rightAttributeName),
              rightTableTuple.getSchema
                .getAttribute(desc.rightAttributeName)
                .getType
            ) == 0
          })
          .map(rightTuple => JoinUtils.joinTuples(tuple, rightTuple))
          .iterator
      } else {
        Iterator()
      }
    } else {
      rightTable += tuple
      if (leftTable.nonEmpty) {
        removeTooSmallTupleInLeftCache(rightTable.head)
        leftTable
          .filter(leftTableTuple => {
            intervalCompare(
              leftTableTuple.getField(desc.leftAttributeName),
              tuple.getField(desc.rightAttributeName),
              leftTableTuple.getSchema
                .getAttribute(desc.leftAttributeName)
                .getType
            ) == 0
          })
          .map(leftTuple => JoinUtils.joinTuples(leftTuple, tuple))
          .iterator
      } else {
        Iterator()
      }
    }

  }

  //if right table has tuple smaller than smallest tuple in left table, delete it
  private def removeTooSmallTupleInRightCache(leftTableSmallestTuple: Tuple): Unit = {
    while (rightTable.nonEmpty) {
      if (
        intervalCompare(
          leftTableSmallestTuple.getField(desc.leftAttributeName),
          rightTable.head.getField(desc.rightAttributeName),
          leftTableSmallestTuple.getSchema
            .getAttribute(desc.leftAttributeName)
            .getType
        ) > 0
      ) {
        rightTable.remove(0)
      } else {
        return
      }
    }

  }

  //if left table has tuple smaller than smallest tuple in right table, delete it
  private def removeTooSmallTupleInLeftCache(rightTableSmallestTuple: Tuple): Unit = {
    while (leftTable.nonEmpty) {
      if (
        intervalCompare(
          leftTable.head.getField(desc.leftAttributeName),
          rightTableSmallestTuple.getField(desc.rightAttributeName),
          rightTableSmallestTuple.getSchema
            .getAttribute(desc.rightAttributeName)
            .getType
        ) < 0
      ) {
        leftTable.remove(0)
      } else {
        return
      }
    }

  }

  private def processNumValue[T](
      pointValue: T,
      leftBoundValue: T,
      rightBoundValue: T
  )(implicit ev$1: T => Ordered[T]): Int = {
    if (desc.includeLeftBound && desc.includeRightBound) {
      if (pointValue >= leftBoundValue && pointValue <= rightBoundValue) 0
      else if (pointValue < leftBoundValue) -1
      else 1
    } else if (desc.includeLeftBound && !desc.includeRightBound) {
      if (pointValue >= leftBoundValue && pointValue < rightBoundValue) 0
      else if (pointValue < leftBoundValue) -1
      else 1
    } else if (!desc.includeLeftBound && desc.includeRightBound) {
      if (pointValue > leftBoundValue && pointValue <= rightBoundValue) 0
      else if (pointValue <= leftBoundValue) -1
      else 1
    } else {
      if (pointValue > leftBoundValue && pointValue < rightBoundValue) 0
      else if (pointValue <= leftBoundValue) -1
      else 1
    }
  }

  private def intervalCompare[K](
      point: K,
      leftBound: K,
      dataType: AttributeType
  ): Int = {
    var result: Int = -2
    if (dataType == AttributeType.LONG) {
      val pointValue: Long = point.asInstanceOf[Long]
      val leftBoundValue: Long = leftBound.asInstanceOf[Long]
      val constantValue: Long = desc.constant
      val rightBoundValue: Long = leftBoundValue + constantValue
      result = processNumValue[Long](
        pointValue,
        leftBoundValue,
        rightBoundValue
      )

    } else if (dataType == AttributeType.DOUBLE) {
      val pointValue: Double = point.asInstanceOf[Double]
      val leftBoundValue: Double = leftBound.asInstanceOf[Double]
      val constantValue: Double = desc.constant.asInstanceOf[Double]
      val rightBoundValue: Double = leftBoundValue + constantValue
      result = processNumValue[Double](
        pointValue,
        leftBoundValue,
        rightBoundValue
      )
    } else if (dataType == AttributeType.INTEGER) {
      val pointValue: Int = point.asInstanceOf[Int]
      val leftBoundValue: Int = leftBound.asInstanceOf[Int]
      val constantValue: Int = desc.constant.asInstanceOf[Int]
      val rightBoundValue: Int = leftBoundValue + constantValue
      result = processNumValue[Int](
        pointValue,
        leftBoundValue,
        rightBoundValue
      )
    } else if (dataType == AttributeType.TIMESTAMP) {
      val pointValue: Timestamp = point.asInstanceOf[Timestamp]
      val leftBoundValue: Timestamp = leftBound.asInstanceOf[Timestamp]
      val rightBoundValue: Timestamp =
        desc.timeIntervalType match {
          case Some(TimeIntervalType.YEAR) =>
            Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusYears(desc.constant))
          case Some(TimeIntervalType.MONTH) =>
            Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusMonths(desc.constant))
          case Some(TimeIntervalType.DAY) =>
            Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusDays(desc.constant))
          case Some(TimeIntervalType.HOUR) =>
            Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusHours(desc.constant))
          case Some(TimeIntervalType.MINUTE) =>
            Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusMinutes(desc.constant))
          case Some(TimeIntervalType.SECOND) =>
            Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusSeconds(desc.constant))
          case None =>
            Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusDays(desc.constant))
        }
      result = processNumValue(
        pointValue.getTime,
        leftBoundValue.getTime,
        rightBoundValue.getTime
      )
    } else {
      throw new WorkflowRuntimeException(s"The data type can not support comparison: $dataType")
    }
    result

  }
}
