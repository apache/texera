package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.operators.aggregate.PartialAggregateOpExec.INTERNAL_AGGREGATE_PARTIAL_OBJECT
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}

import scala.collection.{JavaConverters, mutable}

class FinalAggregateOpExec[Partial <: AnyRef](
    val aggFunc: DistributedAggregation[Partial]
) extends OperatorExecutor {

  var groupByKeyAttributes: Array[Attribute] = _
  var schema: Schema = _

  var partialObjectPerKey = new mutable.HashMap[List[AnyRef], Partial]()
  var outputIterator: Iterator[Tuple] = _

  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        val groupByKey = if (aggFunc == null) null else aggFunc.groupByFunc(t)
        if (groupByKeyAttributes == null) {
          groupByKeyAttributes =
            if (aggFunc == null) Array()
            else groupByKey.getSchema.getAttributes.toArray(new Array[Attribute](0))
        }
        val key =
          if (groupByKey == null) List()
          else JavaConverters.asScalaBuffer(groupByKey.getFields).toList

        val partialObject = t.getField[Partial](INTERNAL_AGGREGATE_PARTIAL_OBJECT)
        if (!partialObjectPerKey.contains(key)) {
          partialObjectPerKey.put(key, partialObject)
        } else {
          partialObjectPerKey.put(key, aggFunc.merge(partialObjectPerKey(key), partialObject))
        }
        Iterator()
      case Right(_) =>
        partialObjectPerKey.iterator.map(pair => {
          val finalObject = aggFunc.finalAgg(pair._2)
          if (schema == null) {
            schema = Schema
              .newBuilder()
              .add(groupByKeyAttributes.toArray: _*)
              .add(finalObject.getSchema)
              .build()
          }
          val fields: Array[Object] =
            (pair._1 ++ JavaConverters.asScalaBuffer(finalObject.getFields)).toArray
          Tuple.newBuilder().add(schema, fields).build()
        })
    }
  }

}
