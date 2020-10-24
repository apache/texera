package edu.uci.ics.texera.web.model.request

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{BreakpointInfo, OperatorLink}

import scala.collection.mutable

case class ExecuteWorkflowRequest(
    operators: mutable.MutableList[OperatorDescriptor],
    links: mutable.MutableList[OperatorLink],
    breakpoints: mutable.MutableList[BreakpointInfo]
) extends TexeraWebSocketRequest
