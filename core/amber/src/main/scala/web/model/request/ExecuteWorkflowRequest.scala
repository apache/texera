package web.model.request

import texera.common.operators.TexeraOperatorDescriptor
import texera.common.workflow.{TexeraBreakpointInfo, TexeraOperatorLink}

import scala.collection.mutable

case class ExecuteWorkflowRequest(
                                   operators: mutable.MutableList[TexeraOperatorDescriptor],
                                   links: mutable.MutableList[TexeraOperatorLink],
                                   breakpoints: mutable.MutableList[TexeraBreakpointInfo]
) extends TexeraWsRequest
