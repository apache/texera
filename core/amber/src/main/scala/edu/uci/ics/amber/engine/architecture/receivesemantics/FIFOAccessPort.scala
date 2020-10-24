package edu.uci.ics.amber.engine.architecture.receivesemantics
import edu.uci.ics.amber.engine.common.ambertag.LayerTag
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.TableMetadata
import akka.actor.ActorRef

import scala.collection.mutable

class FIFOAccessPort {
  val seqNumMap = new mutable.AnyRefMap[ActorRef, Long]
  val stashedMessage =
    new mutable.AnyRefMap[ActorRef, mutable.LongMap[Array[ITuple]]] //can we optimize this?

  val endMap = new mutable.AnyRefMap[ActorRef, Long]
  var endToBeReceived = new mutable.HashMap[LayerTag, mutable.HashSet[ActorRef]]

  val actorToEdge = new mutable.AnyRefMap[ActorRef, LayerTag]

  def isFinished: Boolean = endToBeReceived.isEmpty

  def reset(): Unit = {
    seqNumMap.keys.foreach { sender =>
      seqNumMap(sender) = 0
      stashedMessage(sender) = new mutable.LongMap[Array[ITuple]]
    }
    endMap.clear()
    endToBeReceived.clear()
    actorToEdge.foreach { x =>
      val (sender, from) = x
      if (endToBeReceived.contains(from)) {
        endToBeReceived(from).add(sender)
      } else {
        endToBeReceived(from) = mutable.HashSet[ActorRef](sender)
      }
    }
  }

  def addSender(sender: ActorRef, from: LayerTag): Unit = {
    val edge = actorToEdge.values.find(m => m == from)
    seqNumMap(sender) = 0
    stashedMessage(sender) = new mutable.LongMap[Array[ITuple]]
    if (endToBeReceived.contains(from)) {
      endToBeReceived(from).add(sender)
    } else {
      endToBeReceived(from) = mutable.HashSet[ActorRef](sender)
    }
    actorToEdge(sender) = edge.getOrElse(from)
  }

  def registerEnd(sender: ActorRef, end: Long): Boolean = {
    if (seqNumMap.contains(sender) && seqNumMap(sender) == end) {
      val k = actorToEdge(sender)
      //try{
      //println(sender,end)
      endToBeReceived(k).remove(sender)
      //}catch{
      //  case e:Exception => println(e)
      //}
      if (endToBeReceived(k).isEmpty) {
        endToBeReceived.remove(k)
        true
      } else {
        false
      }
    } else {
      endMap += (sender, end)
      false
    }
  }

  def preCheck(seq: Long, payload: Array[ITuple], sender: ActorRef): Option[Array[Array[ITuple]]] = {
    if (!seqNumMap.contains(sender)) {
      None
    } else {
      var cur = seqNumMap(sender)
      if (cur == seq) {
        //valid, but need to check stashed
        val stashed = stashedMessage(sender)
        val buf = mutable.ArrayBuffer[Array[ITuple]](payload)
        cur += 1
        while (stashed.contains(cur)) {
          buf += stashed(cur)
          stashed.remove(cur)
          cur += 1
        }
        if (endMap.contains(sender) && endMap(sender) == cur) {
          val k = actorToEdge(sender)
          endToBeReceived(k).remove(sender)
          if (endToBeReceived(k).isEmpty) {
            endToBeReceived.remove(k)
            buf += null
          }
        }
        seqNumMap(sender) = cur
        Some(buf.toArray)
      } else if (cur < seq) {
        //put in stashed
        if (!stashedMessage(sender).contains(seq)) {
          stashedMessage(sender) += ((seq, payload))
        }
        None
      } else {
        //de-dup
        None
      }
    }
  }
}
