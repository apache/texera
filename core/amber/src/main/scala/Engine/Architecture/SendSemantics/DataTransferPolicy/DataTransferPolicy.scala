package Engine.Architecture.SendSemantics.DataTransferPolicy

import Engine.Architecture.SendSemantics.Routees.BaseRoutee
import Engine.Common.AmberTag.LinkTag
import Engine.Common.AmberTuple.Tuple
import akka.actor.{Actor, ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

abstract class DataTransferPolicy(var batchSize:Int) extends Serializable {
  var tag:LinkTag = _

  def accept(tuple:Tuple)(implicit sender: ActorRef = Actor.noSender):Unit

  def noMore()(implicit sender: ActorRef = Actor.noSender):Unit

  def pause():Unit

  def resume()(implicit sender:ActorRef):Unit

  def initialize(linkTag:LinkTag, next:Array[BaseRoutee])(implicit ac:ActorContext, sender: ActorRef, timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter):Unit = {
    this.tag = linkTag
    next.foreach(x => log.info("link: {}",x))
  }

  def dispose():Unit

  def reset():Unit

}
