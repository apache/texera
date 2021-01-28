package edu.uci.ics.amber.engine.architecture.principal

import edu.uci.ics.amber.engine.architecture.principal.OperatorState.OperatorState
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

case class OperatorStatistics(
                                operatorState: OperatorState,
                               aggregatedInputRowCount: Long,
                               aggregatedOutputRowCount: Long
)
