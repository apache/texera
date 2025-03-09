package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PhysicalLink}

import java.net.URI

case class ResourceConfig(
    operatorConfigs: Map[PhysicalOpIdentity, OperatorConfig] = Map.empty,
    linkConfigs: Map[PhysicalLink, LinkConfig] = Map.empty,
    portConfigs: Map[GlobalPortIdentity, PortConfig] = Map.empty
)
