package texera.common.operators.source

import Engine.Common.AmberTag.OperatorIdentifier
import Engine.Operators.OpExecConfig

abstract class TexeraSourceOpExecConfig(override val tag: OperatorIdentifier) extends OpExecConfig(tag) {
}
