package texera.common.operators.aggregate

import Engine.Common.{InputExhausted, OperatorExecutor}
import Engine.Common.tuple.Tuple
import Engine.Common.tuple.texera.TexeraTuple
import Engine.Common.tuple.texera.schema.{Attribute, AttributeType, Schema}
import texera.common.workflow.TexeraOperatorExecutor
import texera.common.operators.aggregate.TexeraPartialAggregateOpExec.INTERNAL_AGGREGATE_PARTIAL_OBJECT

import scala.collection.{JavaConverters, mutable}

object TexeraPartialAggregateOpExec {
  val INTERNAL_AGGREGATE_PARTIAL_OBJECT = "__internal_aggregate_partial_object__";
}

class TexeraPartialAggregateOpExec[Partial <: AnyRef](
    val aggFunc: TexeraDistributedAggregation[Partial],
    val groupByKeys: Seq[String]
) extends TexeraOperatorExecutor {

  var groupByKeyAttributes: Array[Attribute] = _
  var schema: Schema = _
  var partialObjectPerKey = new mutable.HashMap[List[AnyRef], Partial]()
  var outputIterator: Iterator[Tuple] = _

  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[TexeraTuple, InputExhausted],
      input: Int
  ): scala.Iterator[TexeraTuple] = {
    tuple match {
      case Left(t) =>
        if (schema == null) {
          groupByKeyAttributes = groupByKeys
            .map(key =>
              JavaConverters
                .asScalaBuffer(t.getSchema.getAttributes)
                .find(a => a.getName.equals(key))
                .get
            )
            .toArray
          schema = Schema
            .newBuilder()
            .add(groupByKeyAttributes.toArray: _*)
            .add(INTERNAL_AGGREGATE_PARTIAL_OBJECT, AttributeType.ANY)
            .build()
        }
        val key = groupByKeys.map(t.getField[AnyRef]).toList
        val partialObject = aggFunc.iterate(partialObjectPerKey.getOrElse(key, aggFunc.init()), t)
        partialObjectPerKey.put(key, partialObject)
        Iterator()
      case Right(_) =>
        partialObjectPerKey.iterator.map(pair => {
          val fields: Array[Object] = (pair._1 :+ pair._2).toArray
          TexeraTuple.newBuilder().add(schema, fields).build()
        })
    }
  }

}
