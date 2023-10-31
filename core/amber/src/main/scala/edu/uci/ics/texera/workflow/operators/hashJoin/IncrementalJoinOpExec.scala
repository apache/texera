package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.Tuple.BuilderV2
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class IncrementalJoinOpExec[K](
    val buildAttributeName: String,
    val probeAttributeName: String,
    val operatorSchemaInfo: OperatorSchemaInfo
) extends OperatorExecutor {

  val leftSchema: Schema = operatorSchemaInfo.inputSchemas(0)
  val rightSchema: Schema = operatorSchemaInfo.inputSchemas(1)

  val leftTuples = new mutable.HashMap[K, (ArrayBuffer[Tuple], Boolean)]()
  val rightTuples = new mutable.HashMap[K, (ArrayBuffer[Tuple], Boolean)]()

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(tuple) =>
        if (input == 0) {
          // left input, find on the right
          val key = tuple.getField(buildAttributeName).asInstanceOf[K]
          val (matchedTuples, _) =
            rightTuples.getOrElse(key, (new ArrayBuffer[Tuple](), false))
          val returnIter = matchedTuples
            .map(right => {
              join(tuple, right)
            })
            .iterator
          building(tuple, input)
          returnIter
        } else {
          // right input, find on the left
          val key = tuple.getField(probeAttributeName).asInstanceOf[K]
          val (matchedTuples, _) =
            leftTuples.getOrElse(key, (new ArrayBuffer[Tuple](), false))
          val returnIter = matchedTuples
            .map(left => {
              join(left, tuple)
            })
            .iterator
          building(tuple, input)
          returnIter
        }
      case Right(_) =>
        Iterator()
    }
  }

  private def join(left: Tuple, right: Tuple): Tuple = {
    val builder = Tuple
      .newBuilder(operatorSchemaInfo.outputSchemas(0))
      .add(left)

    fillNonJoinFields(
      builder,
      rightSchema,
      right.getFields.toArray(),
      resolveDuplicateName = true
    )

    builder.build()
  }

  def fillNonJoinFields(
      builder: BuilderV2,
      schema: Schema,
      fields: Array[Object],
      resolveDuplicateName: Boolean = false
  ): Unit = {
    schema.getAttributesScala.filter(attribute => attribute.getName != probeAttributeName) map {
      (attribute: Attribute) =>
        {
          val field = fields.apply(schema.getIndex(attribute.getName))
          if (resolveDuplicateName) {
            val attributeName = attribute.getName
            builder.add(
              new Attribute(
                if (leftSchema.getAttributeNames.contains(attributeName))
                  attributeName + "#@1"
                else attributeName,
                attribute.getType
              ),
              field
            )
          } else {
            builder.add(attribute, field)
          }
        }
    }
  }

  private def building(tuple: Tuple, input: Int): Unit = {
    val key = tuple.getField(buildAttributeName).asInstanceOf[K]
    if (input == 0) {
      val (storedTuples, _) =
        leftTuples.getOrElseUpdate(key, (new ArrayBuffer[Tuple](), false))
      storedTuples += tuple
    } else {
      val (storedTuples, _) =
        rightTuples.getOrElseUpdate(key, (new ArrayBuffer[Tuple](), false))
      storedTuples += tuple
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

}
