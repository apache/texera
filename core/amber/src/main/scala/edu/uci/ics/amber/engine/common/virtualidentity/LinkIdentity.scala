package edu.uci.ics.amber.engine.common.virtualidentity

case class LinkIdentity(from: LayerIdentity, to: LayerIdentity, inputNum: Int)
    extends VirtualIdentity {
  override def toString: String = s"Link(${from.toString} -> ${to.toString} as Input $inputNum)"
}
