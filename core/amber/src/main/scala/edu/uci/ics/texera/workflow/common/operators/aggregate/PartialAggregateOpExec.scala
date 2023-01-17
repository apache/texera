package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.operators.aggregate.PartialAggregateOpExec.INTERNAL_AGGREGATE_PARTIAL_OBJECT
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.{JavaConverters, mutable}

object PartialAggregateOpExec {
  val INTERNAL_AGGREGATE_PARTIAL_OBJECT = "__internal_aggregate_partial_object__";
}

class PartialAggregateOpExec[Partial <: AnyRef](
    val aggFuncs: List[DistributedAggregation[Partial]]
) extends OperatorExecutor {

  var groupByKeyAttributes: Array[Attribute] = _
  var schema: Schema = _
//  var partialObjectPerKey = new mutable.HashMap[List[AnyRef], Partial]()

  var partialObjectsPerKey = (1 to aggFuncs.length).map(_ => new mutable.HashMap[List[AnyRef], Partial]())
  var outputIterator: Iterator[ITuple] = _

  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): scala.Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        val groupBySchema = if (aggFuncs == null) null else aggFuncs(0).groupByFunc(t.getSchema)
        val builder = Tuple.newBuilder(groupBySchema)
        groupBySchema.getAttributeNames.foreach(attrName =>
          builder.add(t.getSchema.getAttribute(attrName), t.getField(attrName))
        )
        val groupByKey = if (aggFuncs == null) null else builder.build()
        // TODO Find a way to get this from the OpDesc. Since this is generic, trying to get the
        // right schema from there is a bit challenging.
        // See https://github.com/Texera/texera/pull/1166#discussion_r654863854
        if (schema == null) {
          groupByKeyAttributes =
            if (aggFuncs == null) Array()
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

        for ((aggFunc, index) <- aggFuncs.view.zipWithIndex) {
          val partialObject = aggFunc.iterate(partialObjectsPerKey(index).getOrElse(key, aggFunc.init()), t)
          partialObjectsPerKey(index).put(key, partialObject)
        }

        Iterator()
      case Right(_) =>
        (1 to partialObjectsPerKey(0).iterator.length).iterator.map(index => {
          var listFields = partialObjectsPerKey(index).iterator.map(pair => {
            (pair._1 :+ pair._2).toArray
          })
//          val fields: Array[Object] = (pair._1 :+ pair._2).toArray
          var tuple = Tuple.newBuilder(schema)
          listFields.foreach(fields => {
            tuple.addSequentially(fields)
          })
          tuple.build()
//          Tuple.newBuilder(schema).addSequentially(fields).build()
        })
//        partialObjectsPerKey(0).iterator.map(pair => {
//
//        })
    }
  }

}
