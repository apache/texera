package texera.common.operators.source

import Engine.Common.AmberTag.OperatorTag
import Engine.Operators.OpExecConfig

abstract class TexeraSourceOpExecConfig(override val tag: OperatorTag) extends OpExecConfig(tag) {
}
