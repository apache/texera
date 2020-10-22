package edu.uci.ics.texera.workflow.operators.mysqlsource

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.OneOnEach
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{ActorLayer, GeneratorWorkerLayer}
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class MysqlSourceOpExecConfig(
    tag: OperatorIdentifier,
    numWorkers: Int,
    schema: Schema,
    host: String,
    port: String,
    database: String,
    table: String,
    username: String,
    password: String,
    limit: Integer,
    offset: Integer,
    column: String,
    keywords: String
) extends OpExecConfig(tag) {

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new GeneratorWorkerLayer(
          LayerTag(tag, "main"),
          _ => {
            new MysqlSourceOpExec(
              schema,
              host,
              port,
              database,
              table,
              username,
              password,
              limit,
              offset,
              column,
              keywords
            )
          },
          numWorkers,
          UseAll(), // it's source operator
          OneOnEach()
        )
      ),
      Array(),
      Map()
    )
  }

  override def assignBreakpoint(
      topology: Array[ActorLayer],
      states: mutable.AnyRefMap[ActorRef, WorkerState.Value],
      breakpoint: GlobalBreakpoint
)(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter): Unit = {
    breakpoint.partition(topology(0).layer.filter(states(_) != WorkerState.Completed))
  }

}
