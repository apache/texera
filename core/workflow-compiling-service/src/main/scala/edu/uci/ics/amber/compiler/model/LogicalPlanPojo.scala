package edu.uci.ics.amber.compiler.model

import edu.uci.ics.amber.operator.LogicalOp

case class LogicalPlanPojo(
    operators: List[LogicalOp],
    links: List[LogicalLink],
    opsToViewResult: List[String],
    opsToReuseResult: List[String]
)
