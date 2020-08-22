package web.model.request

import texera.common.workflow.{TexeraBreakpointInfo, TexeraOperator, TexeraOperatorLink}

import scala.collection.mutable

case class ExecuteWorkflowRequest(
    operators: mutable.MutableList[TexeraOperator],
    links: mutable.MutableList[TexeraOperatorLink],
    breakpoints: mutable.MutableList[TexeraBreakpointInfo]
) extends TexeraWsRequest
