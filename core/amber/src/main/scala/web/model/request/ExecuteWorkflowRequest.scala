package web.model.request

import texera.common.workflow.{TexeraBreakpointInfo, OperatorDescriptor, TexeraOperatorLink}

import scala.collection.mutable

case class ExecuteWorkflowRequest(
                                   operators: mutable.MutableList[OperatorDescriptor],
                                   links: mutable.MutableList[TexeraOperatorLink],
                                   breakpoints: mutable.MutableList[TexeraBreakpointInfo]
) extends TexeraWsRequest
