package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

abstract class LinkStrategy(
    val from: WorkerLayer,
    val to: WorkerLayer,
    val batchSize: Int,
    val inputNum: Int
) extends Serializable {

  val tag = LinkTag(from.tag, to.tag, inputNum)

  def link()(implicit timeout: Timeout, ec: ExecutionContext): Unit
}
