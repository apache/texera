package edu.uci.ics.amber.engine.common.ambertag

case class WorkerTag(workflow: String, operator: String, layer: String, index: Int)
    extends AmberTag {
  def getLocalIdentity: String = layer + "/" + index

  override def getGlobalIdentity: String = workflow + "-" + operator + "-" + layer + "/" + index
}

object WorkerTag {
  def apply(layerTag: LayerTag, index: Int): WorkerTag = {
    WorkerTag(layerTag.workflow, layerTag.operator, layerTag.layer, index)
  }

  def apply(operatorTag: OperatorIdentifier, layer: String, index: Int): WorkerTag = {
    WorkerTag(operatorTag.workflow, operatorTag.operator, layer, index)
  }

  def apply(workflowTag: WorkflowTag, operator: String, layer: String, index: Int): WorkerTag = {
    WorkerTag(workflowTag.workflow, operator, layer, index)
  }
}
