package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor

import scala.collection.mutable

case class BreakpointInfo(operatorID: String, breakpoint: Breakpoint)

case class WorkflowInfo(
    operators: mutable.MutableList[OperatorDescriptor],
    links: mutable.MutableList[OperatorLink],
    breakpoints: mutable.MutableList[BreakpointInfo]
)
