package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.operators.aggregate.PartialAggregateOpExec.internalAggObjKey
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}

import scala.collection.mutable
import scala.jdk.CollectionConverters.asJavaIterableConverter

object PartialAggregateOpExec {
  val INTERNAL_AGGREGATE_PARTIAL_OBJECT = "__internal_aggregate_partial_object__";
  def internalAggObjKey(key: Int): String = {
    s"__internal_aggregate_partial_object_${key}__"
  }
}

class PartialAggregateOpExec(
    val aggFuncs: List[DistributedAggregation[Object]],
    val groupByKeys: List[String],
    val operatorSchemaInfo: OperatorSchemaInfo
) extends OperatorExecutor {

  var schema: Schema = Schema
    .newBuilder()
    // add group by keys
    .add(groupByKeys.map(k => operatorSchemaInfo.inputSchemas(0).getAttribute(k)).asJava)
    // add intermediate internal aggregation objects
    .add(aggFuncs.indices.map(i => new Attribute(internalAggObjKey(i), AttributeType.ANY)).asJava)
    .build()

  var partialObjectsPerKey = new mutable.HashMap[List[Object], List[Object]]()

  // for incremental computation
  // the time interval that partial aggregate operator emits incremental update to final aggregate
  val UPDATE_INTERVAL_MS = 500
  // the timestamp of the last incremental update
  private var lastUpdatedTime: Long = 0

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
        insertToPartialAggState(t)
        if (shouldEmitOutput()) emitOutputAndResetState() else Iterator()
      case Right(_) =>
        emitOutputAndResetState()
    }
  }

  private def shouldEmitOutput(): Boolean = {
    System.currentTimeMillis - lastUpdatedTime > UPDATE_INTERVAL_MS
  }

  private def emitOutputAndResetState(): scala.Iterator[Tuple] = {
    lastUpdatedTime = System.currentTimeMillis
    val resultIterator = getPartialOutputs()
    this.partialObjectsPerKey = new mutable.HashMap[List[Object], List[Object]]()
    resultIterator
  }

  private def getPartialOutputs(): scala.Iterator[Tuple] = {
    partialObjectsPerKey.iterator.map(pair => {
      val tupleFields = pair._1 ++ pair._2
      Tuple.newBuilder(schema).addSequentially(tupleFields.toArray).build()
    })
  }

  private def insertToPartialAggState(t: Tuple): Unit = {
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

  }

}
