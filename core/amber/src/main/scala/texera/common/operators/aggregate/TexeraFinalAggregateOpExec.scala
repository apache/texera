package texera.common.operators.aggregate

import Engine.Common.InputExhausted
import Engine.Common.tuple.texera.TexeraTuple
import Engine.Common.tuple.texera.schema.{Attribute, Schema}
import texera.common.operators.TexeraOperatorExecutor
import texera.common.operators.aggregate.TexeraPartialAggregateOpExec.INTERNAL_AGGREGATE_PARTIAL_OBJECT

import scala.collection.{JavaConverters, mutable}

class TexeraFinalAggregateOpExec[Partial <: AnyRef](
    val aggFunc: TexeraDistributedAggregation[Partial]
) extends TexeraOperatorExecutor {

  var groupByKeyAttributes: Array[Attribute] = _
  var schema: Schema = _

  var partialObjectPerKey = new mutable.HashMap[List[AnyRef], Partial]()
  var outputIterator: Iterator[TexeraTuple] = _

  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[TexeraTuple, InputExhausted],
      input: Int
  ): Iterator[TexeraTuple] = {
    tuple match {
      case Left(t) =>
        if (groupByKeyAttributes == null) {
          groupByKeyAttributes = aggFunc.groupByKeys
            .map(key =>
              JavaConverters
                .asScalaBuffer(t.getSchema.getAttributes)
                .find(a => a.getName.equals(key))
                .get
            )
            .toArray
        }
        val key = aggFunc.groupByKeys.map(t.getField[AnyRef]).toList
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
          val fields: Array[Object] = (pair._1 ++ JavaConverters.asScalaBuffer(finalObject.getFields)).toArray
          TexeraTuple.newBuilder().add(schema, fields).build()
        })
    }
  }

}
