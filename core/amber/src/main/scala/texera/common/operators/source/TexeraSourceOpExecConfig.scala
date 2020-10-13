package texera.common.operators.source

import engine.common.ambertag.OperatorIdentifier
import engine.operators.OpExecConfig

abstract class TexeraSourceOpExecConfig(override val tag: OperatorIdentifier) extends OpExecConfig(tag) {
}
