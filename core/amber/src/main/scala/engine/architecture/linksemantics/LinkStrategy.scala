package engine.architecture.linksemantics

import engine.architecture.deploysemantics.layer.ActorLayer
import engine.common.ambertag.LinkTag
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

abstract class LinkStrategy(val from: ActorLayer, val to: ActorLayer, val batchSize: Int)
    extends Serializable {

  val tag = LinkTag(from.tag, to.tag)

  def link()(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter): Unit
}
