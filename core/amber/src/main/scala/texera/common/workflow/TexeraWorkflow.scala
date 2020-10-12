package texera.common.workflow

import scala.collection.mutable

case class TexeraBreakpointInfo(operatorID: String, breakpoint: TexeraBreakpoint)

case class TexeraWorkflow(
                           operators: mutable.MutableList[TexeraOperatorDescriptor],
                           links: mutable.MutableList[TexeraOperatorLink],
                           breakpoints: mutable.MutableList[TexeraBreakpointInfo]
)
