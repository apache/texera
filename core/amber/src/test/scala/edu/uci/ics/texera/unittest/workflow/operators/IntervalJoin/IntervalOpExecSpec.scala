package edu.uci.ics.texera.unittest.workflow.operators.IntervalJoin

import java.sql.Timestamp
import java.time.LocalDateTime

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.intervalJoin.{
  IntervalJoinOpDesc,
  IntervalJoinOpExec,
  TimeIntervalType
}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class IntervalOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val left: LinkIdentity = linkID()
  val right: LinkIdentity = linkID()

  var opDesc: IntervalJoinOpDesc = _
  var counter: Int = 0

  def linkID(): LinkIdentity = LinkIdentity(layerID(), layerID())

  def layerID(): LayerIdentity = {
    counter += 1
    LayerIdentity("" + counter, "" + counter, "" + counter)
  }

  def intergerTuple(name: String, n: Int = 1, i: Int): Tuple = {
    Tuple
      .newBuilder(schema(name, AttributeType.INTEGER, n))
      .add(new Attribute(name, AttributeType.INTEGER), i)
      .add(new Attribute(name + "_" + 1, AttributeType.INTEGER), i)
      .build()
  }

  def doubleTuple(name: String, n: Int = 1, i: Double): Tuple = {
    Tuple
      .newBuilder(schema(name, AttributeType.DOUBLE, n))
      .add(new Attribute(name, AttributeType.DOUBLE), i)
      .add(new Attribute(name + "_" + 1, AttributeType.DOUBLE), i)
      .build()
  }

  def longTuple(name: String, n: Int = 1, i: Long): Tuple = {
    Tuple
      .newBuilder(schema(name, AttributeType.LONG, n))
      .add(new Attribute(name, AttributeType.LONG), i)
      .add(new Attribute(name + "_" + 1, AttributeType.LONG), i)
      .build()
  }

  def timeStampTuple(name: String, n: Int = 1, i: Timestamp): Tuple = {
    Tuple
      .newBuilder(schema(name, AttributeType.TIMESTAMP, n))
      .add(new Attribute(name, AttributeType.TIMESTAMP), i)
      .add(new Attribute(name + "_" + 1, AttributeType.TIMESTAMP), i)
      .build()
  }

  def schema(name: String, attributeType: AttributeType, n: Int = 1): Schema = {
    Schema
      .newBuilder()
      .add(
        new Attribute(name, attributeType),
        new Attribute(name + "_" + n, attributeType)
      )
      .build()
  }

  it should "work with Integer value int [] interval, simple test" in {
    opDesc = new IntervalJoinOpDesc()
    opDesc.leftAttributeName = "point_1"
    opDesc.rightAttributeName = "range_1"
    val inputSchemas =
      Array(schema("point", AttributeType.INTEGER), schema("range", AttributeType.INTEGER))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)
    var opExec = new IntervalJoinOpExec(
      left,
      "point_1",
      "range_1",
      OperatorSchemaInfo(inputSchemas, outputSchema),
      3,
      true,
      true,
      TimeIntervalType.DAY
    )
    opExec.open()
    counter = 0
    (1 to 10).map(i => {
      assert(opExec.processTexeraTuple(Left(intergerTuple("point", 1, i)), left).isEmpty)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), left).isEmpty)
    var rangeList = Array(1, 5, 8)
    rangeList.map(i => opExec.processTexeraTuple(Left(intergerTuple("range", 1, i)), right))
    val outputTuples = opExec.processTexeraTuple(Right(InputExhausted()), right).toList
    assert(outputTuples.size == 11)
    assert(outputTuples.head.getSchema.getAttributeNames.size() == 4)
    opExec.close()
  }

  it should "work with Integer value int [] interval, same key" in {
    opDesc = new IntervalJoinOpDesc()
    opDesc.leftAttributeName = "same"
    opDesc.rightAttributeName = "same"
    val inputSchemas =
      Array(schema("same", AttributeType.INTEGER), schema("same", AttributeType.INTEGER))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)
    var opExec = new IntervalJoinOpExec(
      left,
      opDesc.leftAttributeName,
      opDesc.rightAttributeName,
      OperatorSchemaInfo(inputSchemas, outputSchema),
      3,
      true,
      true,
      TimeIntervalType.DAY
    )
    opExec.open()
    counter = 0
    (1 to 10).map(i => {
      assert(opExec.processTexeraTuple(Left(intergerTuple("same", 1, i)), left).isEmpty)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), left).isEmpty)
    var rangeList = Array(1, 5, 8)
    rangeList.map(i => opExec.processTexeraTuple(Left(intergerTuple("same", 1, i)), right))
    var outputTuples = opExec.processTexeraTuple(Right(InputExhausted()), right).toList
    assert(outputTuples.size == 11)
    assert(outputTuples.head.getSchema.getAttributeNames.size() == 4)
    opExec.close()
  }

  it should "work with Integer value int [) interval" in {
    opDesc = new IntervalJoinOpDesc()
    opDesc.leftAttributeName = "point_1"
    opDesc.rightAttributeName = "range_1"
    val inputSchemas =
      Array(schema("point", AttributeType.INTEGER), schema("range", AttributeType.INTEGER))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)
    var opExec = new IntervalJoinOpExec(
      left,
      "point_1",
      "range_1",
      OperatorSchemaInfo(inputSchemas, outputSchema),
      3,
      true,
      false,
      TimeIntervalType.DAY
    )
    opExec.open()
    counter = 0
    (1 to 10).map(i => {
      assert(opExec.processTexeraTuple(Left(intergerTuple("point", 1, i)), left).isEmpty)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), left).isEmpty)
    var rangeList = Array(1, 5, 8)
    rangeList.map(i => opExec.processTexeraTuple(Left(intergerTuple("range", 1, i)), right))
    var outputTuples = opExec.processTexeraTuple(Right(InputExhausted()), right).toList

    assert(outputTuples.size == 9)
    assert(outputTuples.head.getSchema.getAttributeNames.size() == 4)
    opExec.close()
  }

  it should "work with Integer value int (] interval" in {
    opDesc = new IntervalJoinOpDesc()
    opDesc.leftAttributeName = "point_1"
    opDesc.rightAttributeName = "range_1"
    val inputSchemas =
      Array(schema("point", AttributeType.INTEGER), schema("range", AttributeType.INTEGER))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)
    var opExec = new IntervalJoinOpExec(
      left,
      "point_1",
      "range_1",
      OperatorSchemaInfo(inputSchemas, outputSchema),
      3,
      false,
      true,
      TimeIntervalType.DAY
    )
    opExec.open()
    counter = 0
    (1 to 10).map(i => {
      assert(opExec.processTexeraTuple(Left(intergerTuple("point", 1, i)), left).isEmpty)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), left).isEmpty)
    var rangeList = Array(1, 5, 8)
    rangeList.map(i => opExec.processTexeraTuple(Left(intergerTuple("range", 1, i)), right))
    var outputTuples = opExec.processTexeraTuple(Right(InputExhausted()), right).toList
    assert(outputTuples.size == 8)
    assert(outputTuples.head.getSchema.getAttributeNames.size() == 4)
    opExec.close()
  }
  it should "work with Integer value int () interval" in {
    opDesc = new IntervalJoinOpDesc()
    opDesc.leftAttributeName = "point_1"
    opDesc.rightAttributeName = "range_1"
    val inputSchemas =
      Array(schema("point", AttributeType.INTEGER), schema("range", AttributeType.INTEGER))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)
    var opExec = new IntervalJoinOpExec(
      left,
      "point_1",
      "range_1",
      OperatorSchemaInfo(inputSchemas, outputSchema),
      3,
      false,
      false,
      TimeIntervalType.DAY
    )
    opExec.open()
    counter = 0
    (1 to 10).map(i => {
      assert(opExec.processTexeraTuple(Left(intergerTuple("point", 1, i)), left).isEmpty)

    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), left).isEmpty)
    var rangeList = Array(1, 5, 8)
    rangeList.map(i => opExec.processTexeraTuple(Left(intergerTuple("range", 1, i)), right))
    var outputTuples = opExec.processTexeraTuple(Right(InputExhausted()), right).toList

    assert(outputTuples.size == 6)
    assert(outputTuples.head.getSchema.getAttributeNames.size() == 4)
    opExec.close()
  }
  it should "work with Timestamp value int [] interval" in {
    opDesc = new IntervalJoinOpDesc()
    opDesc.leftAttributeName = "point_1"
    opDesc.rightAttributeName = "range_1"
    val inputSchemas =
      Array(schema("point", AttributeType.TIMESTAMP), schema("range", AttributeType.TIMESTAMP))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)
    var opExec = new IntervalJoinOpExec(
      left,
      "point_1",
      "range_1",
      OperatorSchemaInfo(inputSchemas, outputSchema),
      3,
      true,
      true,
      TimeIntervalType.DAY
    )
    opExec.open()
    counter = 0
    var localDateTime: LocalDateTime = LocalDateTime.of(2021, 7, 1, 0, 0, 0, 0)
    var pointList: Array[Long] = Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)
    pointList.foreach(i => {
      assert(
        opExec
          .processTexeraTuple(
            Left(timeStampTuple("point", 1, Timestamp.valueOf(localDateTime.plusDays(i)))),
            left
          )
          .isEmpty
      )
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), left).isEmpty)
    var rangeList: Array[Long] = Array(1L, 5L, 8L)
    rangeList
      .map(i =>
        opExec.processTexeraTuple(
          Left(timeStampTuple("range", 1, Timestamp.valueOf(localDateTime.plusDays(i)))),
          right
        )
      )
    var outputTuples = opExec.processTexeraTuple(Right(InputExhausted()), right).toList

    assert(outputTuples.size == 11)
    assert(outputTuples.head.getSchema.getAttributeNames.size() == 4)
    opExec.close()
  }

  it should "work with Double value int [] interval" in {
    opDesc = new IntervalJoinOpDesc()
    opDesc.leftAttributeName = "point_1"
    opDesc.rightAttributeName = "range_1"
    val inputSchemas =
      Array(schema("point", AttributeType.DOUBLE), schema("range", AttributeType.DOUBLE))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)
    var opExec = new IntervalJoinOpExec(
      left,
      "point_1",
      "range_1",
      OperatorSchemaInfo(inputSchemas, outputSchema),
      3,
      true,
      true,
      TimeIntervalType.DAY
    )
    opExec.open()
    counter = 0
    var pointList: Array[Double] = Array(1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1, 8.1, 9.1, 10.1)
    pointList.foreach(i => {
      assert(
        opExec.processTexeraTuple(Left(doubleTuple("point", 1, i)), left).isEmpty
      )
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), left).isEmpty)
    var rangeList: Array[Double] = Array(1.1, 5.1, 8.1)
    rangeList.map(i => opExec.processTexeraTuple(Left(doubleTuple("range", 1, i)), right))
    val outputTuples = opExec.processTexeraTuple(Right(InputExhausted()), right).toList

    assert(outputTuples.size == 11)
    assert(outputTuples.head.getSchema.getAttributeNames.size() == 4)
    opExec.close()
  }

  it should "work with Long value int [] interval" in {
    opDesc = new IntervalJoinOpDesc()
    opDesc.leftAttributeName = "point_1"
    opDesc.rightAttributeName = "range_1"
    val inputSchemas =
      Array(schema("point", AttributeType.LONG), schema("range", AttributeType.LONG))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)
    var opExec = new IntervalJoinOpExec(
      left,
      "point_1",
      "range_1",
      OperatorSchemaInfo(inputSchemas, outputSchema),
      3,
      true,
      true,
      TimeIntervalType.DAY
    )
    opExec.open()
    counter = 0
    var pointList: Array[Long] = Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)
    pointList.foreach(i => {
      assert(
        opExec.processTexeraTuple(Left(longTuple("point", 1, i)), left).isEmpty
      )
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), left).isEmpty)
    var rangeList: Array[Long] = Array(1L, 5L, 8L)
    rangeList.map(i => opExec.processTexeraTuple(Left(longTuple("range", 1, i)), right))
    val outputTuples = opExec.processTexeraTuple(Right(InputExhausted()), right).toList
    assert(outputTuples.size == 11)
    assert(outputTuples.head.getSchema.getAttributeNames.size() == 4)
    opExec.close()
  }

  it should "work with basic two input streams with left empty table" in {
    opDesc = new IntervalJoinOpDesc()
    opDesc.leftAttributeName = "point_1"
    opDesc.rightAttributeName = "range_1"
    val inputSchemas =
      Array(schema("point", AttributeType.INTEGER), schema("range", AttributeType.INTEGER))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)
    var opExec = new IntervalJoinOpExec(
      left,
      "point_1",
      "range_1",
      OperatorSchemaInfo(inputSchemas, outputSchema),
      3,
      true,
      true,
      TimeIntervalType.DAY
    )
    opExec.open()
    counter = 0
    var pointList: Array[Int] = Array()
    pointList.foreach(i => {
      assert(opExec.processTexeraTuple(Left(intergerTuple("point", 1, i)), left).isEmpty)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), left).isEmpty)
    var rangeList = Array(1, 5, 8)
    rangeList.map(i => opExec.processTexeraTuple(Left(intergerTuple("range", 1, i)), right))
    val outputTuples  = opExec.processTexeraTuple(Right(InputExhausted()), right)
    assert(outputTuples.isEmpty)
    opExec.close()
  }

  it should "work with basic two input streams with right empty table" in {
    opDesc = new IntervalJoinOpDesc()
    opDesc.leftAttributeName = "point_1"
    opDesc.rightAttributeName = "range_1"
    val inputSchemas =
      Array(schema("point", AttributeType.INTEGER), schema("range", AttributeType.INTEGER))
    val outputSchema = opDesc.getOutputSchema(inputSchemas)
    var opExec = new IntervalJoinOpExec(
      left,
      "point_1",
      "range_1",
      OperatorSchemaInfo(inputSchemas, outputSchema),
      3,
      true,
      true,
      TimeIntervalType.DAY
    )
    opExec.open()
    counter = 0
    var pointList: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    pointList.foreach(i => {
      assert(opExec.processTexeraTuple(Left(intergerTuple("point", 1, i)), left).isEmpty)
    })
    assert(opExec.processTexeraTuple(Right(InputExhausted()), left).isEmpty)
    var rangeList: Array[Int] = Array()
    val outputTuples =
      rangeList.map(i => opExec.processTexeraTuple(Left(intergerTuple("range", 1, i)), right))
    assert(opExec.processTexeraTuple(Right(InputExhausted()), right).isEmpty)

    assert(outputTuples.isEmpty)
    opExec.close()
  }

}
