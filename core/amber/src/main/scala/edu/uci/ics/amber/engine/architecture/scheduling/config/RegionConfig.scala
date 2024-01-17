package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.engine.common.virtualidentity.{PhysicalLinkIdentity, PhysicalOpIdentity}

case class RegionConfig(
    operatorConfigs: Map[PhysicalOpIdentity, OperatorConfig],
    linkConfigs: Map[PhysicalLinkIdentity, LinkConfig]
)
