package engine.architecture.principal

import engine.architecture.principal.PrincipalState.PrincipalState
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

class PrincipalStateType extends TypeReference[PrincipalState.type]

case class PrincipalStatistics(
    @JsonScalaEnumeration(classOf[PrincipalStateType]) operatorState: PrincipalState,
    aggregatedInputRowCount: Long,
    aggregatedOutputRowCount: Long
)
