package Engine.Common.AmberTag

case class LayerTag(workflow:String,operator:String,layer:String) extends AmberTag {

  override def getGlobalIdentity: String = workflow+"-"+operator+"-"+layer
}

object LayerTag{
  def apply(workflowTag: WorkflowTag,operator:String,layer:String):LayerTag = {
    LayerTag(workflowTag.workflow,operator,layer)
  }

  def apply(operatorTag: OperatorTag,layer:String):LayerTag = {
    LayerTag(operatorTag.workflow,operatorTag.operator,layer)
  }
}
