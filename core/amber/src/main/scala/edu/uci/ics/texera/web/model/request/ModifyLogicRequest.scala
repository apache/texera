package edu.uci.ics.texera.web.model.request

import edu.uci.ics.texera.workflow.common.operators.TexeraOperatorDescriptor

case class ModifyLogicRequest(
    operator: TexeraOperatorDescriptor
) extends TexeraWsRequest
