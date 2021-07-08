package edu.uci.ics.texera.workflow.operators.intervalJoin

import java.sql.Timestamp

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}

import scala.collection.mutable.ArrayBuffer

class IntervalJoinOpExec[K](
                             val isLeftTableInput: LinkIdentity,
                             val leftTableAttributeName: String,
                             val rightTableAttributeName: String,
                             val operatorSchemaInfo: OperatorSchemaInfo,
                             val constant: Long,
                             val includeLeftBound: Boolean,
                             val includeRightBound: Boolean,
                             var timeIntervalType:TimeIntervalType
                           ) extends OperatorExecutor {

  var ispointTableFinished: Boolean = false
  var outputProbeSchema: Schema = _
  var leftTable: ArrayBuffer[Tuple] = _
  var rangeTable: ArrayBuffer[Tuple] = _
  val leftTableSchema: Schema = operatorSchemaInfo.inputSchemas(0)
  val rightTableSchema: Schema = operatorSchemaInfo.inputSchemas(1)

  private def createOutputProbeSchema(): Schema = {
    var builder = Schema.newBuilder()
    rightTableSchema
      .getAttributes
      .forEach(attr => {
        if (leftTableSchema.containsAttribute(attr.getName)) {
          builder.add(new Attribute(s"${attr.getName}#@1", attr.getType))
        } else {
          builder.add(attr)
        }
      })
    builder.build()
  }

  override def processTexeraTuple(
                                   tuple: Either[Tuple, InputExhausted],
                                   input: LinkIdentity
                                 ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        if (input == isLeftTableInput) {
          leftTable += t
          Iterator()
        } else {
          rangeTable += t
          var tuplesToOutput: ArrayBuffer[Tuple] = new ArrayBuffer[Tuple]()
          if (leftTable.isEmpty) {
            Iterator()
          }
          leftTable.foreach(pointTuple => {
            if (
              joinConditionHolds(
                includeLeftBound,
                includeRightBound,
                pointTuple.getField(leftTableAttributeName),
                t.getField(rightTableAttributeName),
                constant,
                operatorSchemaInfo.inputSchemas(0).getAttribute(leftTableAttributeName).getType
              )
            ) {
              val builder = Tuple
                .newBuilder(operatorSchemaInfo.outputSchema)
                .add(pointTuple)
              for (i <- 0 until t.getFields.size()) {
                val attributeName = t.getSchema.getAttributeNames.get(i)
                val attribute = t.getSchema.getAttribute(attributeName)
                builder.add(
                  new Attribute(
                    if (leftTableSchema.getAttributeNames.contains(attributeName))
                      attributeName + "#@1"
                    else attributeName,
                    attribute.getType
                  ),
                  t.getFields.get(i)
                )
              }
              tuplesToOutput += builder.build()
            }
          })
          tuplesToOutput.iterator
        }
      case Right(_) =>
        if (input == isLeftTableInput) {
          ispointTableFinished = true
        }
        Iterator()
    }
  }

  def joinConditionHolds(
                      includeLeftBound: Boolean,
                      includerightBound: Boolean,
                      point: K,
                      leftBound: K,
                      constant: Long,
                      dataType: AttributeType
                    ): Boolean = {
    if (dataType == AttributeType.LONG) {
      var pointValue: Long = point.asInstanceOf[Long]
      var leftBoundValue: Long = leftBound.asInstanceOf[Long]
      var constantValue: Long = constant.asInstanceOf[Long]
      var rightBoundValue: Long = leftBoundValue + constantValue
      if (includeLeftBound && includerightBound) {
        pointValue >= leftBoundValue && pointValue <= rightBoundValue
      } else if (includeLeftBound && !includerightBound) {
        pointValue >= leftBoundValue && pointValue < rightBoundValue
      } else if (!includeLeftBound && includerightBound) {
        pointValue > leftBoundValue && pointValue <= rightBoundValue
      } else {
        pointValue > leftBoundValue && pointValue < rightBoundValue
      }
    } else if (dataType == AttributeType.DOUBLE) {
      var pointValue: Double = point.asInstanceOf[Double]
      var leftBoundValue: Double = leftBound.asInstanceOf[Double]
      var constantValue: Double = constant.asInstanceOf[Double]
      var rightBoundValue: Double = leftBoundValue + constantValue
      if (includeLeftBound && includerightBound) {
        pointValue >= leftBoundValue && pointValue <= rightBoundValue
      } else if (includeLeftBound && !includerightBound) {
        pointValue >= leftBoundValue && pointValue < rightBoundValue
      } else if (!includeLeftBound && includerightBound) {
        pointValue > leftBoundValue && pointValue <= rightBoundValue
      } else {
        pointValue > leftBoundValue && pointValue < rightBoundValue
      }
    } else if (dataType == AttributeType.INTEGER) {
      var pointValue: Int = point.asInstanceOf[Int]
      var leftBoundValue: Int = leftBound.asInstanceOf[Int]
      var constantValue: Int = constant.asInstanceOf[Int]
      var rightBoundValue: Int = leftBoundValue + constantValue
      if (includeLeftBound && includerightBound) {
        pointValue >= leftBoundValue && pointValue <= rightBoundValue
      } else if (includeLeftBound && !includerightBound) {
        pointValue >= leftBoundValue && pointValue < rightBoundValue
      } else if (!includeLeftBound && includerightBound) {
        pointValue > leftBoundValue && pointValue <= rightBoundValue
      } else {
        pointValue > leftBoundValue && pointValue < rightBoundValue
      }
    } else if (dataType == AttributeType.TIMESTAMP) {
      var pointValue: Timestamp = point.asInstanceOf[Timestamp]
      var leftBoundValue: Timestamp = leftBound.asInstanceOf[Timestamp]
      var rightBoundValue:Timestamp =
        timeIntervalType match {
          case TimeIntervalType.YEAR => Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusYears(constant))
          case TimeIntervalType.MONTH => Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusMonths(constant))
          case TimeIntervalType.DAY => Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusDays(constant))
          case TimeIntervalType.HOUR => Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusHours(constant))
          case TimeIntervalType.MINUTE => Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusMinutes(constant))
          case TimeIntervalType.SECOND => Timestamp.valueOf(leftBoundValue.toLocalDateTime.plusSeconds(constant))
        }
      if (includeLeftBound && includerightBound) {
        (pointValue.after(leftBoundValue) || pointValue.equals(leftBoundValue)) && (pointValue
          .before(rightBoundValue) || pointValue.equals(rightBoundValue))
      } else if (includeLeftBound && !includerightBound) {
        (pointValue.after(leftBoundValue) || pointValue.equals(leftBoundValue)) && pointValue
          .before(rightBoundValue)
      } else if (!includeLeftBound && includerightBound) {
        pointValue.after(leftBoundValue) && (pointValue.before(rightBoundValue) || pointValue
          .equals(rightBoundValue))
      } else {
        pointValue.after(leftBoundValue) && pointValue.before(rightBoundValue)
      }
    } else {
      val err = WorkflowRuntimeError(
        "The data type can not support comparation: " + dataType.toString,
        "HashJoinOpExec",
        Map("stacktrace" -> Thread.currentThread().getStackTrace.mkString("\n"))
      )
      throw new WorkflowRuntimeException(err)
    }
  }

  override def open(): Unit = {
    outputProbeSchema = createOutputProbeSchema()
    leftTable = new ArrayBuffer[Tuple]()
    rangeTable = new ArrayBuffer[Tuple]()
  }

  override def close(): Unit = {
    leftTable.clear()
    rangeTable.clear()
  }
}
