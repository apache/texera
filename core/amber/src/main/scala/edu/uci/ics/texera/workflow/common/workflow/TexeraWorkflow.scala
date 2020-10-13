package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.texera.workflow.common.operators.TexeraOperatorDescriptor

import scala.collection.mutable

case class TexeraBreakpointInfo(operatorID: String, breakpoint: TexeraBreakpoint)

case class TexeraWorkflow(
                           operators: mutable.MutableList[TexeraOperatorDescriptor],
                           links: mutable.MutableList[TexeraOperatorLink],
                           breakpoints: mutable.MutableList[TexeraBreakpointInfo]
)
