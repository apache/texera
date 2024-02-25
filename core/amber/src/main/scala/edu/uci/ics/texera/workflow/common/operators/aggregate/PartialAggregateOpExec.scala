package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.operators.aggregate.PartialAggregateOpExec.getOutputSchema
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

import scala.collection.mutable
import scala.jdk.CollectionConverters.IterableHasAsJava

object PartialAggregateOpExec {
  def internalAggObjKey(key: Int): String = {
    s"__internal_aggregate_partial_object_${key}__"
  }

  def getOutputSchema(
      inputSchema: Schema,
      aggFuncs: List[DistributedAggregation[Object]],
      groupByKeys: List[String]
  ): Schema = {
    Schema
      .newBuilder()
      // add group by keys
      .add(groupByKeys.map(k => inputSchema.getAttribute(k)).asJava)
      // add intermediate internal aggregation objects
      .add(aggFuncs.indices.map(i => new Attribute(internalAggObjKey(i), AttributeType.ANY)).asJava)
      .build()
  }
}

class PartialAggregateOpExec(
    val aggFuncs: List[DistributedAggregation[Object]],
    val groupByKeys: List[String],
    val inputSchema: Schema
) extends OperatorExecutor {

  var schema: Schema = getOutputSchema(inputSchema, aggFuncs, groupByKeys)

  var partialObjectsPerKey = new mutable.HashMap[List[Object], List[Object]]()

  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): scala.Iterator[Tuple] = {
    if (aggFuncs.isEmpty) {
      throw new UnsupportedOperationException("Aggregation Functions Cannot be Empty")
    }
    tuple match {
      case Left(t) =>
        val key =
          if (groupByKeys == null || groupByKeys.isEmpty) List()
          else groupByKeys.map(k => t.getField[Object](k))

        if (!partialObjectsPerKey.contains(key))
          partialObjectsPerKey.put(key, aggFuncs.map(aggFunc => aggFunc.init()))

        val partialObjects =
          partialObjectsPerKey.getOrElseUpdate(key, aggFuncs.map(aggFunc => aggFunc.init()))
        val updatedPartialObjects = aggFuncs.zip(partialObjects).map {
          case (aggFunc, partial) =>
            aggFunc.iterate(partial, t)
        }
        partialObjectsPerKey.put(key, updatedPartialObjects)
        Iterator()
      case Right(_) =>
        partialObjectsPerKey.iterator.map(pair => {
          val tupleFields = pair._1 ++ pair._2
          Tuple.newBuilder(schema).addSequentially(tupleFields.toArray).build()
        })
    }
  }

}
