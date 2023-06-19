package edu.uci.ics.texera.workflow.operators.cartesianProduct

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.Tuple.BuilderV2
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}

import scala.collection.mutable.ArrayBuffer

class CartesianProductOpExec(operatorSchemaInfo: OperatorSchemaInfo) extends OperatorExecutor {
  val leftSchemaSize: Int = operatorSchemaInfo.inputSchemas(0).getAttributesScala.length
  val rightSchema: Schema = operatorSchemaInfo.inputSchemas(1)
  var leftTuples: ArrayBuffer[Tuple] = _
  var isLeftTupleCollectionFinished: Boolean = false
  var outputSchema: Schema = operatorSchemaInfo.outputSchemas(0)

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      // is Tuple
      case Left(tuple) =>
        if (input == 0) {
          // left port, store the tuple
          leftTuples += tuple
          Iterator()
        } else if (!isLeftTupleCollectionFinished) {
          // should not occur, have to finish processing left input tuples
          throw new WorkflowRuntimeException(
            "Cannot process right input port tuples before finished with left input port"
          )
        } else {
          // right port, join with all left tuples
          leftTuples
            .map(leftTuple => {
              /*
              output schema should have unchanged left tuple attributes
              followed by right schema attributes (with duplicates renamed)
               */
              val builder = Tuple.newBuilder(outputSchema).add(leftTuple)
              fillRightTupleFields(
                builder,
                rightSchema,
                tuple.getFields.toArray(),
                leftSchemaSize,
                resolveDuplicateName = true
              )
              builder.build()
            })
            .toIterator
        }

      // is InputExhausted
      case Right(_) =>
        if (input == 0 && !isLeftTupleCollectionFinished) {
          // mark as completed with processing left tuples
          isLeftTupleCollectionFinished = true
        }
        Iterator()
    }
  }

  // add attributes of a "right" tuple to existing Tuple builder
  private def fillRightTupleFields(
      builder: BuilderV2,
      rightSchema: Schema,
      fields: Array[Object],
      leftSchemaOffset: Int,
      resolveDuplicateName: Boolean = false
  ): Unit = {
    val outputSchemas = outputSchema.getAttributesScala
    rightSchema.getAttributesScala map { (attribute: Attribute) =>
      {
        val attributeIndex = rightSchema.getIndex(attribute.getName)
        val field = fields.apply(attributeIndex)
        if (resolveDuplicateName) {
          // duplicate attribute name, use renamed attribute generated in output schema
          builder.add(outputSchemas.apply(leftSchemaOffset + attributeIndex), field)
        } else {
          builder.add(attribute, field)
        }
      }
    }
  }

  override def open(): Unit = {
    leftTuples = new ArrayBuffer[Tuple]()
  }

  override def close(): Unit = {
    leftTuples.clear()
  }
}
