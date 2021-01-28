package edu.uci.ics.amber.engine.architecture.worker.neo

import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalBreakpointTriggeredHandler.LocalBreakpointTriggered
import edu.uci.ics.amber.engine.architecture.worker.neo.BreakpointManager.{BreakpointSnapshot, LocalBreakpointInfo}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.tuple.ITuple

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

object BreakpointManager{
  final case class BreakpointSnapshot(currentTuple:ITuple, breakpoints: Array[LocalBreakpointInfo])

  final case class LocalBreakpointInfo(breakpoint:LocalBreakpoint, triggeredByCurrentTuple: Boolean)
}



class BreakpointManager(asyncRPCClient: AsyncRPCClient) {
  private var breakpoints = new Array[LocalBreakpoint](0)
  private val triggeredBreakpointIndices = new mutable.HashSet[Int]

  def hasBreakpointTriggered: Boolean = triggeredBreakpointIndices.nonEmpty

  def registerOrReplaceBreakpoint(breakpoint: LocalBreakpoint): Unit = {
    var i = 0
    Breaks.breakable {
      while (i < breakpoints.length) {
        if (breakpoints(i).id == breakpoint.id) {
          triggeredBreakpointIndices.remove(i)
          breakpoints(i) = breakpoint
          Breaks.break()
        }
        i += 1
      }
      breakpoints = breakpoints :+ breakpoint
    }
  }

  def getBreakpoints(tuple:ITuple, ids:Array[String]): BreakpointSnapshot ={
    BreakpointSnapshot(tuple, ids.map(id => {
      val idx = breakpoints.indexWhere(_.id == id)
      LocalBreakpointInfo(breakpoints(idx), triggeredBreakpointIndices.contains(idx))
    }))
  }

  def removeBreakpoint(breakpointID: String): Unit = {
    val idx = breakpoints.indexWhere(_.id == breakpointID)
    if (idx != -1) {
      triggeredBreakpointIndices.remove(idx)
      breakpoints = breakpoints.take(idx)
    }
  }

  def evaluateTuple(tuple:ITuple): Boolean ={
    var isTriggered = false
    var triggeredBreakpoints:ArrayBuffer[(String,Long)] = null
    breakpoints.indices.foreach{
      i =>
        if(breakpoints(i).checkCondition(tuple)){
          isTriggered = true
          triggeredBreakpointIndices.add(i)
          if(triggeredBreakpoints == null){
            triggeredBreakpoints = ArrayBuffer[(String, Long)]()
          }else{
            triggeredBreakpoints.append((breakpoints(i).id, breakpoints(i).version))
          }
        }
    }
    if(isTriggered){
      asyncRPCClient.send(LocalBreakpointTriggered(triggeredBreakpoints.toArray), VirtualIdentity.Controller)
    }
    isTriggered
  }

}
