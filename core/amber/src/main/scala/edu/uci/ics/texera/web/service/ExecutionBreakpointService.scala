package edu.uci.ics.texera.web.service

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.{
  ConditionalGlobalBreakpoint,
  CountGlobalBreakpoint
}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.BreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.AssignBreakpointHandler.AssignGlobalBreakpoint
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.texera.web.SubscriptionManager
import edu.uci.ics.texera.web.model.websocket.event.{ReportFaultedTupleEvent, TexeraWebSocketEvent}
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.texera.web.workflowruntimestate.BreakpointFault.BreakpointTuple
import edu.uci.ics.texera.web.workflowruntimestate.{BreakpointFault, OperatorBreakpoints}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.PAUSED
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.{
  Breakpoint,
  BreakpointCondition,
  ConditionBreakpoint,
  CountBreakpoint
}

import scala.collection.mutable

class ExecutionBreakpointService(
    client: AmberClient,
    stateStore: ExecutionStateStore
) extends SubscriptionManager {

  registerCallbackOnBreakpoint()

  addSubscription(
    stateStore.breakpointStore.registerDiffHandler { (oldState, newState) =>
      {
        val output = new mutable.ArrayBuffer[TexeraWebSocketEvent]()
        newState.operatorInfo
          .foreach {
            case (opId, info) =>
              val oldInfo = oldState.operatorInfo.getOrElse(opId, new OperatorBreakpoints())
              if (
                info.unresolvedBreakpoints.nonEmpty && info.unresolvedBreakpoints != oldInfo.unresolvedBreakpoints
              ) {
                output.append(ReportFaultedTupleEvent(info.unresolvedBreakpoints, opId))
              }
          }
        output
      }
    }
  )

  /** *
    *  Callback Functions to register upon construction
    */
  private[this] def registerCallbackOnBreakpoint(): Unit = {
    addSubscription(
      client
        .registerCallback[BreakpointTriggered]((evt: BreakpointTriggered) => {
          stateStore.metadataStore.updateState { oldState =>
            updateWorkflowState(PAUSED, oldState)
          }
          stateStore.breakpointStore.updateState { breakpointStore =>
            val breakpointEvts = evt.faultedTupleMap.map { elem =>
              val workerName = elem._1.name
              val faultedTuple = elem._2
              val tupleList =
                if (faultedTuple.tuple != null) {
                  faultedTuple.tuple.toArray().filter(v => v != null).map(v => v.toString).toList
                } else {
                  List.empty
                }
              BreakpointFault(
                workerName,
                Some(BreakpointTuple(faultedTuple.id, faultedTuple.isInput, tupleList))
              )
            }.toArray
            val newInfo = breakpointStore.operatorInfo
              .getOrElse(evt.operatorID, OperatorBreakpoints())
              .withUnresolvedBreakpoints(breakpointEvts)
            breakpointStore.addOperatorInfo((evt.operatorID, newInfo))
          }
        })
    )
  }

  def clearTriggeredBreakpoints(): Unit = {
    stateStore.breakpointStore.updateState { breakpointStore =>
      breakpointStore.withOperatorInfo(
        breakpointStore.operatorInfo.map(pair =>
          pair._1 -> pair._2.withUnresolvedBreakpoints(Seq.empty)
        )
      )
    }
  }

  def addBreakpoint(operatorID: String, breakpoint: Breakpoint): Future[_] = {
    val breakpointID = "breakpoint-" + operatorID + "-" + System.currentTimeMillis()
    breakpoint match {
      case conditionBp: ConditionBreakpoint =>
        val column = conditionBp.column
        val predicate: Tuple => Boolean = conditionBp.condition match {
          case BreakpointCondition.EQ =>
            tuple => {
              tuple.getField(column).toString.trim == conditionBp.value
            }
          case BreakpointCondition.LT =>
            tuple => tuple.getField(column).toString.trim < conditionBp.value
          case BreakpointCondition.LE =>
            tuple => tuple.getField(column).toString.trim <= conditionBp.value
          case BreakpointCondition.GT =>
            tuple => tuple.getField(column).toString.trim > conditionBp.value
          case BreakpointCondition.GE =>
            tuple => tuple.getField(column).toString.trim >= conditionBp.value
          case BreakpointCondition.NE =>
            tuple => tuple.getField(column).toString.trim != conditionBp.value
          case BreakpointCondition.CONTAINS =>
            tuple => tuple.getField(column).toString.trim.contains(conditionBp.value)
          case BreakpointCondition.NOT_CONTAINS =>
            tuple => !tuple.getField(column).toString.trim.contains(conditionBp.value)
        }

        client.sendAsync(
          AssignGlobalBreakpoint(
            new ConditionalGlobalBreakpoint(
              breakpointID,
              tuple => {
                val texeraTuple = tuple.asInstanceOf[Tuple]
                predicate.apply(texeraTuple)
              }
            ),
            operatorID
          )
        )
      case countBp: CountBreakpoint =>
        client.sendAsync(
          AssignGlobalBreakpoint(new CountGlobalBreakpoint(breakpointID, countBp.count), operatorID)
        )
    }
  }

}
