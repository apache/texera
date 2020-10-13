package edu.uci.ics.texera.web.model.request

import edu.uci.ics.texera.workflow.common.operators.TexeraOperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{TexeraBreakpointInfo, TexeraOperatorLink}

import scala.collection.mutable

case class TexeraWorkflow(
    operators: mutable.MutableList[TexeraOperatorDescriptor],
    links: mutable.MutableList[TexeraOperatorLink],
    breakpoints: mutable.MutableList[TexeraBreakpointInfo]
)
