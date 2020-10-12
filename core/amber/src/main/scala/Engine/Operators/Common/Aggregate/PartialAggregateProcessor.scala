package Engine.Operators.Common.Aggregate

import Engine.Common.{InputExhausted, OperatorExecutor}
import Engine.Common.tuple.Tuple
import Engine.Common.tuple.texera.TexeraTuple
import Engine.Common.tuple.texera.schema.{Attribute, AttributeType, Schema}
import Engine.Operators.Common.Aggregate.PartialAggregateProcessor.{INTERNAL_AGGREGATE_KEY, INTERNAL_AGGREGATE_PARTIAL_OBJECT}

import scala.collection.{JavaConverters, mutable}

object PartialAggregateProcessor{
  val INTERNAL_AGGREGATE_KEY = "__internal_aggregate_key__";
  val INTERNAL_AGGREGATE_PARTIAL_OBJECT = "__internal_aggregate_partial_object__";
}

class PartialAggregateProcessor[Partial <: AnyRef, Final <: AnyRef](
    val aggFunc: DistributedAggregation[Partial, Final],
    val groupByKeys: Seq[String]
) extends OperatorExecutor {
  var groupByKeyAttributes: Seq[Attribute] = _
  var partialObjectPerKey = new mutable.HashMap[Seq[Any], Partial]()
  var outputIterator: Iterator[Tuple] = _

  val schema: Schema = Schema
    .newBuilder()
    .add(INTERNAL_AGGREGATE_KEY, AttributeType.ANY)
    .add(INTERNAL_AGGREGATE_PARTIAL_OBJECT, AttributeType.ANY)
    .build()

  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTuple(tuple: Either[Tuple, InputExhausted], input: Int): scala.Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        val texeraTuple = t.asInstanceOf[TexeraTuple]
        if (groupByKeyAttributes == null) {
          groupByKeyAttributes = groupByKeys.map(key =>
            JavaConverters.asScalaBuffer(texeraTuple.getSchema.getAttributes).find(a => a.getName.equals(key)).get)
        }

        val key = groupByKeys.map(texeraTuple.getField)
        val partialObject = aggFunc.iterate(partialObjectPerKey.getOrElse(key, aggFunc.init()), texeraTuple)
        partialObjectPerKey.put(key, partialObject)
        Iterator()
      case Right(_) =>
        partialObjectPerKey.iterator.map(pair => {
          val fields: Array[Object] = Array(pair._1, pair._2)
          TexeraTuple.newBuilder().add(schema, fields).build()
        })
    }
  }

}
