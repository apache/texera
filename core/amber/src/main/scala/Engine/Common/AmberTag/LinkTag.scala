package Engine.Common.AmberTag

case class LinkTag(from:LayerTag, to:LayerTag) extends AmberTag {
  override def getGlobalIdentity: String = from.getGlobalIdentity+"-=-"+to.getGlobalIdentity
}