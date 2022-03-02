package edu.uci.ics.texera.web.service

import com.twitter.util.{Duration, Future}
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.{
  ConditionalGlobalBreakpoint,
  CountGlobalBreakpoint
}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.BreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.AssignBreakpointHandler.AssignGlobalBreakpoint
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.texera.web.SubscriptionManager
import edu.uci.ics.texera.web.model.websocket.event.{BreakpointTriggeredEvent, TexeraWebSocketEvent}
import edu.uci.ics.texera.web.storage.{JobStateStore, WorkflowStateStore}
import edu.uci.ics.texera.web.workflowruntimestate.BreakpointFault.BreakpointTuple
import edu.uci.ics.texera.web.workflowruntimestate.{
  BreakpointFault,
  OperatorBreakpoints,
  OperatorRuntimeStats,
  PythonOperatorInfo
}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.PAUSED
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.{
  Breakpoint,
  BreakpointCondition,
  ConditionBreakpoint,
  CountBreakpoint
}

import scala.collection.mutable

class JobBreakpointService(
    client: AmberClient,
    stateStore: JobStateStore
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
                output.append(BreakpointTriggeredEvent(info.unresolvedBreakpoints, opId))
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
          stateStore.jobMetadataStore.updateState { oldState =>
            oldState.withState(PAUSED)
          }
          stateStore.breakpointStore.updateState { jobInfo =>
            val breakpointEvts = evt.report
              .filter(_._1._2 != null)
              .map { elem =>
                val actorPath = elem._1._1.toString
                val faultedTuple = elem._1._2
                val tupleList =
                  if (faultedTuple.tuple != null) {
                    faultedTuple.tuple.toArray().filter(v => v != null).map(v => v.toString).toList
                  } else {
                    List.empty
                  }
                BreakpointFault(
                  actorPath,
                  Some(BreakpointTuple(faultedTuple.id, faultedTuple.isInput, tupleList)),
                  elem._2
                )
              }
              .toArray
            val newInfo = jobInfo.operatorInfo
              .getOrElse(evt.operatorID, OperatorBreakpoints())
              .withUnresolvedBreakpoints(breakpointEvts)
            jobInfo.addOperatorInfo((evt.operatorID, newInfo))
          }
        })
    )
  }

  def clearTriggeredBreakpoints(): Unit = {
    stateStore.breakpointStore.updateState { jobInfo =>
      jobInfo.withOperatorInfo(
        jobInfo.operatorInfo.map(pair => pair._1 -> pair._2.withUnresolvedBreakpoints(Seq.empty))
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
