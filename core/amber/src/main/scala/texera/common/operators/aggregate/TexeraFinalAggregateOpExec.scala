package texera.common.operators.aggregate

import engine.common.InputExhausted
import texera.common.operators.TexeraOperatorExecutor
import texera.common.operators.aggregate.TexeraPartialAggregateOpExec.INTERNAL_AGGREGATE_PARTIAL_OBJECT
import texera.common.tuple.TexeraTuple
import texera.common.tuple.schema.{Attribute, Schema}

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
          val fields: Array[Object] = (pair._1 ++ JavaConverters.asScalaBuffer(finalObject.getFields)).toArray
          TexeraTuple.newBuilder().add(schema, fields).build()
        })
    }
  }

}
