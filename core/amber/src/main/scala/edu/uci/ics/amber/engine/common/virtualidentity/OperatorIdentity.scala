package edu.uci.ics.amber.engine.common.virtualidentity

object OperatorIdentity {
  def apply(workflowIdentity: WorkflowIdentity, operatorID: String): OperatorIdentity = {
    OperatorIdentity(workflowIdentity.id, operatorID)
  }
}

case class OperatorIdentity(workflow: String, operator: String) extends VirtualIdentity {
  override def toString: String = s"Operator($workflow,$operator)"
}
