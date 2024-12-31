package edu.uci.ics.amber.operator.intervalJoin

import edu.uci.ics.amber.core.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}
import edu.uci.ics.amber.core.workflow.{PhysicalLink, PortIdentity}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer
import scala.util.Random.{nextInt, nextLong}
import edu.uci.ics.amber.core.tuple.{
  Attribute,
  AttributeType,
  Schema,
  SchemaEnforceable,
  Tuple,
  TupleLike
}
import edu.uci.ics.amber.util.JSONUtils.objectMapper
class IntervalOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val left: Int = 0
  val right: Int = 1

  var opDesc: IntervalJoinOpDesc = _
  var counter: Int = 0

  def physicalLink(): PhysicalLink =
    PhysicalLink(
      physicalOpId(),
      fromPortId = PortIdentity(),
      physicalOpId(),
      toPortId = PortIdentity()
    )

  def physicalOpId(): PhysicalOpIdentity = {
    counter += 1
    PhysicalOpIdentity(OperatorIdentity("" + counter), "" + counter)
  }

  def newTuple[T](name: String, n: Int = 1, i: T, attributeType: AttributeType): Tuple = {
    Tuple
      .builder(schema(name, attributeType, n))
      .add(new Attribute(name, attributeType), i)
      .add(new Attribute(name + "_" + 1, attributeType), i)
      .build()
  }

  def integerTuple(name: String, n: Int = 1, i: Int): Tuple = {
    Tuple
      .builder(schema(name, AttributeType.INTEGER, n))
      .add(new Attribute(name, AttributeType.INTEGER), i)
      .add(new Attribute(name + "_" + 1, AttributeType.INTEGER), i)
      .build()
  }

  def doubleTuple(name: String, n: Int = 1, i: Double): Tuple = {
    Tuple
      .builder(schema(name, AttributeType.DOUBLE, n))
      .add(new Attribute(name, AttributeType.DOUBLE), i)
      .add(new Attribute(name + "_" + 1, AttributeType.DOUBLE), i)
      .build()
  }

  def schema(name: String, attributeType: AttributeType, n: Int = 1): Schema = {
    Schema
      .builder()
      .add(
        new Attribute(name, attributeType),
        new Attribute(name + "_" + n, attributeType)
      )
      .build()
  }

  def longTuple(name: String, n: Int = 1, i: Long): Tuple = {
    Tuple
      .builder(schema(name, AttributeType.LONG, n))
      .add(new Attribute(name, AttributeType.LONG), i)
      .add(new Attribute(name + "_" + 1, AttributeType.LONG), i)
      .build()
  }

  def timeStampTuple(name: String, n: Int = 1, i: Timestamp): Tuple = {
    Tuple
      .builder(schema(name, AttributeType.TIMESTAMP, n))
      .add(new Attribute(name, AttributeType.TIMESTAMP), i)
      .add(new Attribute(name + "_" + 1, AttributeType.TIMESTAMP), i)
      .build()
  }

  def bruteForceJoin[T](
      leftInput: Array[T],
      rightInput: Array[T],
      includeLeftBound: Boolean,
      includeRightBound: Boolean,
      constant: Long,
      dataType: AttributeType,
      timeIntervalType: TimeIntervalType = TimeIntervalType.DAY
  ): Int = {
    var resultSize: Int = 0
    for (k <- leftInput.indices) {
      for (i <- rightInput.indices) {
        dataType match {
          case AttributeType.INTEGER =>
            if (
              compare(
                leftInput(k).asInstanceOf[Int].toLong,
                rightInput(i).asInstanceOf[Int].toLong,
                rightInput(i).asInstanceOf[Int].toLong + constant,
                includeLeftBound,
                includeRightBound
              )
            ) {
              resultSize += 1
            }
          case AttributeType.LONG =>
            if (
              compare(
                leftInput(k).asInstanceOf[Long],
                rightInput(i).asInstanceOf[Long],
                rightInput(i).asInstanceOf[Long] + constant,
                includeLeftBound,
                includeRightBound
              )
            ) {
              resultSize += 1
            }
          case AttributeType.TIMESTAMP =>
            val leftBoundValue: Long = rightInput(i).asInstanceOf[Timestamp].getTime
            val rightBoundValue: Long =
              timeIntervalType match {
                case TimeIntervalType.YEAR =>
                  Timestamp
                    .valueOf(
                      rightInput(i).asInstanceOf[Timestamp].toLocalDateTime.plusYears(constant)
                    )
                    .getTime
                case TimeIntervalType.MONTH =>
                  Timestamp
                    .valueOf(
                      rightInput(i).asInstanceOf[Timestamp].toLocalDateTime.plusMonths(constant)
                    )
                    .getTime
                case TimeIntervalType.DAY =>
                  Timestamp
                    .valueOf(
                      rightInput(i).asInstanceOf[Timestamp].toLocalDateTime.plusDays(constant)
                    )
                    .getTime
                case TimeIntervalType.HOUR =>
                  Timestamp
                    .valueOf(
                      rightInput(i).asInstanceOf[Timestamp].toLocalDateTime.plusHours(constant)
                    )
                    .getTime
                case TimeIntervalType.MINUTE =>
                  Timestamp
                    .valueOf(
                      rightInput(i).asInstanceOf[Timestamp].toLocalDateTime.plusMinutes(constant)
                    )
                    .getTime
                case TimeIntervalType.SECOND =>
                  Timestamp
                    .valueOf(
                      rightInput(i).asInstanceOf[Timestamp].toLocalDateTime.plusSeconds(constant)
                    )
                    .getTime
              }
            if (
              compare(
                leftInput(k).asInstanceOf[Timestamp].getTime,
                leftBoundValue,
                rightBoundValue,
                includeLeftBound,
                includeRightBound
              )
            ) {
              resultSize += 1
            }
          case _ => throw new RuntimeException(s"unexpected type $dataType")
        }
      }
    }
    resultSize
  }

  def compare(
      input1: Long,
      leftBound: Long,
      rightBound: Long,
      includeLeftBound: Boolean,
      includeRightBound: Boolean
  ): Boolean = {
    if (includeLeftBound && includeRightBound) {
      input1 >= leftBound && input1 <= rightBound
    } else if (includeLeftBound && !includeRightBound) {
      input1 >= leftBound && input1 < rightBound
    } else if (!includeLeftBound && includeRightBound) {
      input1 > leftBound && input1 <= rightBound
    } else {
      input1 > leftBound && input1 < rightBound
    }
  }

  def testJoin[T](
      leftKey: String,
      rightKey: String,
      includeLeftBound: Boolean,
      includeRightBound: Boolean,
      dataType: AttributeType,
      timeIntervalType: TimeIntervalType,
      intervalConstant: Long,
      leftInput: Array[T],
      rightInput: Array[T]
  ): Unit = {
    val inputSchemas =
      Map(
        PortIdentity() -> schema(leftKey, dataType),
        PortIdentity(1) -> schema(rightKey, dataType)
      )
    opDesc = new IntervalJoinOpDesc(
      leftKey,
      rightKey,
      intervalConstant,
      includeLeftBound,
      includeRightBound,
      timeIntervalType
    )
    val outputSchema = opDesc.getExternalOutputSchemas(inputSchemas).values.head
    val opExec = new IntervalJoinOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    counter = 0
    var leftIndex: Int = 0
    var rightIndex: Int = 0
    val leftOrder = LazyList.continually(nextInt(10)).take(leftInput.length).toList
    val rightOrder = LazyList.continually(nextInt(10)).take(rightInput.length).toList
    val outputTuples: ArrayBuffer[Tuple] = new ArrayBuffer[Tuple]

    while (leftIndex < leftOrder.size || rightIndex < rightOrder.size) {
      if (
        leftIndex < leftOrder.size && (rightIndex >= rightOrder.size || leftOrder(
          leftIndex
        ) < rightOrder(rightIndex))
      ) {
        val result = opExec
          .processTuple(newTuple[T](leftKey, 1, leftInput(leftIndex), dataType), left)
          .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
          .toBuffer
        outputTuples.appendAll(
          result
        )
        leftIndex += 1
      } else if (rightIndex < rightOrder.size) {
        val result = opExec
          .processTuple(newTuple(rightKey, 1, rightInput(rightIndex), dataType), right)
          .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
          .toBuffer
        outputTuples.appendAll(
          result
        )
        rightIndex += 1
      }
    }
    val bruteForceResult: Int = bruteForceJoin(
      leftInput,
      rightInput,
      includeLeftBound,
      includeRightBound,
      intervalConstant,
      dataType
    )
    assert(outputTuples.size == bruteForceResult)
    assert(opExec.onFinish(left).isEmpty)
    assert(opExec.onFinish(right).isEmpty)
    if (outputTuples.nonEmpty)
      assert(outputTuples.head.getSchema.getAttributeNames.length == 4)
    opExec.close()
  }

  it should "random order test" in {

    val pointList: Array[Long] = (1L to 10L).toArray[Long]
    val rangeList: Array[Long] = Array(1L, 5L, 8L)
    testJoin[Long](
      "point",
      "range",
      includeLeftBound = true,
      includeRightBound = true,
      AttributeType.LONG,
      TimeIntervalType.DAY,
      3,
      pointList,
      rangeList
    )
  }
  it should "work with Integer value int [] interval, simple test" in {
    val pointList: Array[Int] = (1 to 10).toArray[Int]
    val rangeList: Array[Int] = Array(1, 5, 8)
    testJoin[Int](
      "point",
      "range",
      includeLeftBound = true,
      includeRightBound = true,
      AttributeType.INTEGER,
      TimeIntervalType.DAY,
      3,
      pointList,
      rangeList
    )
  }

  it should "work with Integer value int [] interval, same key" in {

    val pointList: Array[Int] = (1 to 10).toArray[Int]
    val rangeList: Array[Int] = Array(1, 5, 8)
    testJoin[Int](
      "same",
      "same",
      includeLeftBound = true,
      includeRightBound = true,
      AttributeType.INTEGER,
      TimeIntervalType.DAY,
      3,
      pointList,
      rangeList
    )
  }

  it should "work with Integer value int [) interval" in {

    val pointList: Array[Int] = (1 to 10).toArray[Int]
    val rangeList: Array[Int] = Array(1, 5, 8)
    testJoin[Int](
      "point",
      "range",
      includeLeftBound = true,
      includeRightBound = false,
      AttributeType.INTEGER,
      TimeIntervalType.DAY,
      3,
      pointList,
      rangeList
    )
  }

  it should "work with Integer value int (] interval" in {

    val pointList: Array[Int] = (1 to 10).toArray[Int]
    val rangeList: Array[Int] = Array(1, 5, 8)
    testJoin[Int](
      "point",
      "range",
      includeLeftBound = false,
      includeRightBound = true,
      AttributeType.INTEGER,
      TimeIntervalType.DAY,
      3,
      pointList,
      rangeList
    )
  }
  it should "work with Integer value int () interval" in {

    val pointList: Array[Int] = (1 to 10).toArray[Int]
    val rangeList: Array[Int] = Array(1, 5, 8)
    testJoin[Int](
      "point",
      "range",
      includeLeftBound = false,
      includeRightBound = false,
      AttributeType.INTEGER,
      TimeIntervalType.DAY,
      3,
      pointList,
      rangeList
    )
  }
  it should "work with Timestamp value int [] interval" in {
    val pointList: Array[Timestamp] = (1L to 10L)
      .map(i => {
        new Timestamp(i)
      })
      .toArray[Timestamp]
    val rangeList: Array[Timestamp] = Array(1, 5, 8).map(i => {
      new Timestamp(i)
    })
    testJoin[Timestamp](
      "point",
      "range",
      includeLeftBound = false,
      includeRightBound = true,
      AttributeType.TIMESTAMP,
      TimeIntervalType.DAY,
      3,
      pointList,
      rangeList
    )
  }

  it should "work with Double value int [] interval" in {
    opDesc.leftAttributeName = "point_1"
    opDesc.rightAttributeName = "range_1"
    opDesc.includeLeftBound = true
    opDesc.includeRightBound = true
    opDesc.constant = 3
    opDesc.timeIntervalType = Option(TimeIntervalType.DAY)
    val opExec = new IntervalJoinOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    counter = 0
    val pointList: Array[Double] = Array(1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1, 9.1, 10.1)
    pointList.foreach(i => {
      assert(
        opExec.processTuple(doubleTuple("point", 1, i), left).isEmpty
      )
    })
    assert(opExec.onFinish(left).isEmpty)
    val rangeList: Array[Double] = Array(1.1, 5.1, 8.1)
    val outputTuples = rangeList
      .map(i => opExec.processTuple(doubleTuple("range", 1, i), right))
      .foldLeft(Iterator[TupleLike]())(_ ++ _)
      .toList
    assert(outputTuples.size == 11)
    assert(outputTuples.head.getFields.length == 4)
    opExec.close()
  }

  it should "work with Long value int [] interval" in {

    val pointList: Array[Long] = (1L to 10L).toArray
    val rangeList: Array[Long] = Array(1L, 5L, 8L)
    testJoin[Long](
      "point",
      "range",
      includeLeftBound = true,
      includeRightBound = true,
      AttributeType.LONG,
      TimeIntervalType.DAY,
      3,
      pointList,
      rangeList
    )
  }

  it should "work with basic two input streams with left empty table" in {

    val pointList: Array[Long] = Array()
    val rangeList: Array[Long] = Array(1L, 5L, 8L)
    testJoin[Long](
      "point",
      "range",
      includeLeftBound = true,
      includeRightBound = true,
      AttributeType.LONG,
      TimeIntervalType.DAY,
      3,
      pointList,
      rangeList
    )
  }

  it should "work with basic two input streams with right empty table" in {
    val pointList: Array[Long] = (1L to 10L).toArray
    val rangeList: Array[Long] = Array()
    testJoin[Long](
      "point",
      "range",
      includeLeftBound = true,
      includeRightBound = true,
      AttributeType.LONG,
      TimeIntervalType.DAY,
      3,
      pointList,
      rangeList
    )
  }

  it should "test larger dataset(1k)" in {
    val pointList: Array[Long] = LazyList.continually(nextLong()).take(1000).toArray
    val rangeList: Array[Long] = LazyList.continually(nextLong()).take(1000).toArray
    testJoin[Long](
      "point",
      "range",
      includeLeftBound = true,
      includeRightBound = true,
      AttributeType.LONG,
      TimeIntervalType.DAY,
      nextInt(1000).toLong,
      pointList,
      rangeList
    )
  }

}
