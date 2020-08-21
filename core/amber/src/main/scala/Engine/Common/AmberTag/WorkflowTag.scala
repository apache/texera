package Engine.Common.AmberTag

case class WorkflowTag(workflow:String) extends AmberTag {
  override def getGlobalIdentity: String = workflow
}
