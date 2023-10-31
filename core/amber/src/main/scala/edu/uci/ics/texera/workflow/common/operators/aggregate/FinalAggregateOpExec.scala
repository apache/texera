package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.ProgressiveUtils.{addInsertionFlag, addRetractionFlag}
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.operators.aggregate.PartialAggregateOpExec.internalAggObjKey
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}

import scala.collection.mutable

class FinalAggregateOpExec(
    val aggFuncs: List[DistributedAggregation[Object]],
    val groupByKeys: List[String],
    val operatorSchemaInfo: OperatorSchemaInfo
) extends OperatorExecutor {

  var groupByKeyAttributes: Array[Attribute] = _
  var outputSchema: Schema = operatorSchemaInfo.outputSchemas(0)

  // each value in partialObjectsPerKey has the same length as aggFuncs
  // partialObjectsPerKey(key)[i] corresponds to aggFuncs[i]
  private var partialObjectsPerKey = Map[List[Object], List[Object]]()

  // for incremental computation
  // the time interval that aggregate operator emits incremental update to downstream
  val UPDATE_INTERVAL_MS = 1000
  // the timestamp of the last incremental update
  private var lastUpdatedTime: Long = 0
  // the aggregation state at the last output, used to compute diff with the next output
  private var previousAggResults = Map[List[Object], List[Object]]()

  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    if (aggFuncs.isEmpty) {
      throw new UnsupportedOperationException("Aggregation Functions Cannot be Empty")
    }
    tuple match {
      case Left(t) =>
        insertToFinalAggState(t)
        if (shouldEmitOutput()) emitDiffAndUpdateState() else Iterator()
      case Right(_) =>
        emitDiffAndUpdateState()
    }
  }

  private def shouldEmitOutput(): Boolean = {
    System.currentTimeMillis - lastUpdatedTime > UPDATE_INTERVAL_MS
  }

  private def emitDiffAndUpdateState(): Iterator[Tuple] = {
    val resultIterator = calculateDiff()
    // reset last updated time and previous output results
    lastUpdatedTime = System.currentTimeMillis
    // saves the current aggregation state,
    // note that partialObjectsPerKey is an immutable map variable
    // subsequent updates will change the map pointed by var, but not change the old map
    previousAggResults = partialObjectsPerKey
    resultIterator
  }

  private def calculateDiff(): Iterator[Tuple] = {
    // find differences between the previous and the current aggregation state
    val retractions = new mutable.ArrayBuffer[Tuple]()
    val insertions = new mutable.ArrayBuffer[Tuple]()

    partialObjectsPerKey.keySet.foreach(k => {
      if (!previousAggResults.contains(k)) {
        // this key doesn't exist in the previous state, emit as an insertion tuple
        val newFields = finalAggregate(k, partialObjectsPerKey(k))
        insertions.append(addInsertionFlag(newFields, outputSchema))
      } else if (previousAggResults(k) != partialObjectsPerKey(k)) {
        // this key already exists in the state and its value has changed
        // first retract the previously emitted value, then emit an insertion of the new value
        val prevFields = finalAggregate(k, previousAggResults(k))
        retractions.append(addRetractionFlag(prevFields, outputSchema))
        val newFields = finalAggregate(k, partialObjectsPerKey(k))
        insertions.append(addInsertionFlag(newFields, outputSchema))
      }
    })

    val results = retractions ++ insertions

    results.iterator
  }

  // apply partial aggregation's incremental update to the final aggregation state
  private def insertToFinalAggState(t: Tuple): Unit = {
    val key =
      if (groupByKeys == null || groupByKeys.isEmpty) List()
      else groupByKeys.map(k => t.getField[Object](k))

    val partialObjects =
      aggFuncs.indices.map(i => t.getField[Object](internalAggObjKey(i))).toList
    if (!partialObjectsPerKey.contains(key)) {
      partialObjectsPerKey += (key -> partialObjects)
    } else {
      val updatedPartialObjects = aggFuncs.indices
        .map(i => {
          val aggFunc = aggFuncs(i)
          val partial1 = partialObjectsPerKey(key)(i)
          val partial2 = partialObjects(i)
          aggFunc.merge(partial1, partial2)
        })
        .toList
      partialObjectsPerKey += (key -> updatedPartialObjects)
    }
  }

  private def finalAggregate(key: List[Object], value: List[Object]): Array[Object] = {
    val finalAggValues = aggFuncs.indices.map(i => aggFuncs(i).finalAgg(value(i)))
    (key ++ finalAggValues).toArray
  }

}
