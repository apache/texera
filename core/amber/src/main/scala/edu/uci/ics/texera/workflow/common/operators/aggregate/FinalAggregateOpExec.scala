package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.operators.aggregate.PartialAggregateOpExec.INTERNAL_AGGREGATE_PARTIAL_OBJECT
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.{JavaConverters, mutable}

class FinalAggregateOpExec[Partial <: AnyRef](
    val aggFuncs: List[DistributedAggregation[Partial]]
) extends OperatorExecutor {

  var groupByKeyAttributes: Array[Attribute] = _
  var schema: Schema = _

//  var partialObjectPerKey = new mutable.HashMap[List[AnyRef], Partial]()
  var partialObjectsPerKey = (1 to aggFuncs.length).map(_ => new mutable.HashMap[List[AnyRef], Partial]())
  var outputIterator: Iterator[Tuple] = _

  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        val groupBySchema = if (aggFuncs(0) == null) null else aggFuncs(0).groupByFunc(t.getSchema)
        val builder = Tuple.newBuilder(groupBySchema)
        groupBySchema.getAttributeNames.foreach(attrName =>
          builder.add(t.getSchema.getAttribute(attrName), t.getField(attrName))
        )
        val groupByKey = if (aggFuncs == null) null else builder.build()
        if (groupByKeyAttributes == null) {
          groupByKeyAttributes =
            if (aggFuncs == null) Array()
            else groupByKey.getSchema.getAttributes.toArray(new Array[Attribute](0))
        }
        val key =
          if (groupByKey == null) List()
          else JavaConverters.asScalaBuffer(groupByKey.getFields).toList

        for ((aggFunc, index) <- aggFuncs.view.zipWithIndex) {
          val partialObject = t.getField[Partial](INTERNAL_AGGREGATE_PARTIAL_OBJECT + index)
          if (!partialObjectsPerKey(index).contains(key)) {
            partialObjectsPerKey(index).put(key, partialObject)
          } else {
            partialObjectsPerKey(index).put(key, aggFunc.merge(partialObjectsPerKey(index)(key), partialObject))
          }
        }
        Iterator()
      case Right(_) =>
        var temp = partialObjectsPerKey(0).iterator.map(pair => {
          var tupleBuilder: Tuple.BuilderV2 = null
          partialObjectsPerKey.indices.foreach(index => {
            tupleBuilder = aggFuncs(index).finalAgg(partialObjectsPerKey(index)(pair._1), tupleBuilder)
          })
          val finalObject = tupleBuilder.build()

          val fields: Array[Object] =
            (pair._1 ++ JavaConverters.asScalaBuffer(finalObject.getFields)).toArray

          // TODO Find a way to get this from the OpDesc. Since this is generic, trying to get the
          // right schema from there is a bit challenging.
          // See https://github.com/Texera/texera/pull/1166#discussion_r654863854
          if (schema == null) {
            schema = Schema
              .newBuilder()
              .add(groupByKeyAttributes.toArray: _*)
              .add(finalObject.getSchema)
              .build()
          }

          Tuple.newBuilder(schema).addSequentially(fields).build()

        })

        temp
    }
  }

}
