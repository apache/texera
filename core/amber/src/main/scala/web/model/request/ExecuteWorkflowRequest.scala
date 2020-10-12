package web.model.request

import texera.common.workflow.{TexeraBreakpointInfo, TexeraOperatorDescriptor, TexeraOperatorLink}

import scala.collection.mutable

case class ExecuteWorkflowRequest(
                                   operators: mutable.MutableList[TexeraOperatorDescriptor],
                                   links: mutable.MutableList[TexeraOperatorLink],
                                   breakpoints: mutable.MutableList[TexeraBreakpointInfo]
) extends TexeraWsRequest
