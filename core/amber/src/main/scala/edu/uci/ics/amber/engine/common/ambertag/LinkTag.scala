package edu.uci.ics.amber.engine.common.ambertag

case class LinkTag(from: LayerTag, to: LayerTag) extends AmberTag {
  override def getGlobalIdentity: String = from.getGlobalIdentity + "-=-" + to.getGlobalIdentity
}
