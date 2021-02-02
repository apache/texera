package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.operators.aggregate.PartialAggregateOpExec.INTERNAL_AGGREGATE_PARTIAL_OBJECT
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

import scala.collection.{JavaConverters, mutable}

object PartialAggregateOpExec {
  val INTERNAL_AGGREGATE_PARTIAL_OBJECT = "__internal_aggregate_partial_object__";
}

class PartialAggregateOpExec[Partial <: AnyRef](
    val aggFunc: DistributedAggregation[Partial]
) extends OperatorExecutor {

  var groupByKeyAttributes: Array[Attribute] = _
  var schema: Schema = _
  var partialObjectPerKey = new mutable.HashMap[List[AnyRef], Partial]()
  var outputIterator: Iterator[ITuple] = _

  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): scala.Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        val groupByKey = if (aggFunc == null) null else aggFunc.groupByFunc(t)
        if (schema == null) {
          groupByKeyAttributes =
            if (aggFunc == null) Array()
            else groupByKey.getSchema.getAttributes.toArray(new Array[Attribute](0))
          schema = Schema
            .newBuilder()
            .add(groupByKeyAttributes.toArray: _*)
            .add(INTERNAL_AGGREGATE_PARTIAL_OBJECT, AttributeType.ANY)
            .build()
        }
        val key =
          if (groupByKey == null) List()
          else JavaConverters.asScalaBuffer(groupByKey.getFields).toList

        val partialObject = aggFunc.iterate(partialObjectPerKey.getOrElse(key, aggFunc.init()), t)
        partialObjectPerKey.put(key, partialObject)
        Iterator()
      case Right(_) =>
        partialObjectPerKey.iterator.map(pair => {
          val fields: Array[Object] = (pair._1 :+ pair._2).toArray
          Tuple.newBuilder().add(schema, fields).build()
        })
    }
  }

}
