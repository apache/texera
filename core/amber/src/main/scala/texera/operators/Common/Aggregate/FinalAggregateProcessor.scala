package texera.operators.Common.Aggregate

import Engine.Common.InputExhausted
import Engine.Common.tuple.texera.TexeraTuple
import Engine.Common.tuple.texera.schema.{Attribute, Schema}
import texera.common.workflow.TexeraOperatorExecutor
import texera.operators.Common.Aggregate.PartialAggregateProcessor.INTERNAL_AGGREGATE_PARTIAL_OBJECT

import scala.collection.{JavaConverters, mutable}

class FinalAggregateProcessor[Partial <: AnyRef](
    val aggFunc: DistributedAggregation[Partial],
    val groupByKeys: Seq[String]
) extends TexeraOperatorExecutor {

  var groupByKeyAttributes: Array[Attribute] = _
  var schema: Schema = _

  var partialObjectPerKey = new mutable.HashMap[Array[AnyRef], Partial]()
  var outputIterator: Iterator[TexeraTuple] = _

  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTuple(tuple: Either[TexeraTuple, InputExhausted], input: Int): Iterator[TexeraTuple] = {
    tuple match {
      case Left(t) =>
        if (groupByKeyAttributes == null) {
          groupByKeyAttributes = groupByKeys.map(key =>
            JavaConverters.asScalaBuffer(t.getSchema.getAttributes).find(a => a.getName.equals(key)).get).toArray
        }
        val key = groupByKeys.map(t.getField[AnyRef]).toArray
        val partialObject = t.getField[Partial](INTERNAL_AGGREGATE_PARTIAL_OBJECT)
        if (! partialObjectPerKey.contains(key)) {
          partialObjectPerKey.put(key, partialObject)
        } else {
          partialObjectPerKey.put(key, aggFunc.merge(partialObjectPerKey(key), partialObject))
        }
        Iterator()
      case Right(_) =>
        partialObjectPerKey.iterator.map(pair => {
          val finalObject = aggFunc.finalAgg(pair._2)
          if (schema == null) {
            schema = Schema.newBuilder().add(groupByKeyAttributes.toArray:_*)
              .add(finalObject.getSchema).build()
          }
          val fields: Array[Object] = pair._1 :+ finalObject.getFields
          TexeraTuple.newBuilder().add(schema, fields).build()
        })
    }
  }

}
