package edu.uci.ics.texera.workflow.common.operators.source

import edu.uci.ics.amber.engine.common.ambertag.OperatorIdentifier
import edu.uci.ics.amber.engine.operators.OpExecConfig

abstract class SourceOpExecConfig(override val tag: OperatorIdentifier) extends OpExecConfig(tag) {}
