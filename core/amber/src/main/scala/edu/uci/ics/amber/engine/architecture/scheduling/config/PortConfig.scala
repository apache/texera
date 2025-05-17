package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import java.net.URI

/** Common API for both input ports and output ports. */
sealed trait PortConfig {
  def storageURIs: List[URI]
  def partitioningsOpt: Option[List[Partitioning]]
}

/** Output-port config: exactly one destination URI, no partitioning. */
final case class OutputPortConfig(storageURI: URI) extends PortConfig {
  override val storageURIs: List[URI] = List(storageURI)
  override val partitioningsOpt: Option[List[Partitioning]] = None
}

/**
  * Input-port config.
  * To reach each upstream materialization, a pair of (URI, Partitioning) is needed.
  */
final case class InputPortConfig(
    storagePairs: List[(URI, Option[Partitioning])]
) extends PortConfig {
  override val storageURIs: List[URI] =
    storagePairs.map(_._1)

  override val partitioningsOpt: Option[List[Partitioning]] =
    Some(storagePairs.flatMap(_._2))
}
