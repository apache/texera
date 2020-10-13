package texera.common.operators.aggregate

import engine.common.InputExhausted
import engine.common.tuple.Tuple
import texera.common.operators.TexeraOperatorExecutor
import texera.common.operators.aggregate.TexeraPartialAggregateOpExec.INTERNAL_AGGREGATE_PARTIAL_OBJECT
import texera.common.tuple.TexeraTuple
import texera.common.tuple.schema.{Attribute, AttributeType, Schema}

import scala.collection.{JavaConverters, mutable}

object TexeraPartialAggregateOpExec {
  val INTERNAL_AGGREGATE_PARTIAL_OBJECT = "__internal_aggregate_partial_object__";
}

class TexeraPartialAggregateOpExec[Partial <: AnyRef](
    val aggFunc: TexeraDistributedAggregation[Partial]
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
          TexeraTuple.newBuilder().add(schema, fields).build()
        })
    }
  }

}
