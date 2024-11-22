package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.workflow.PhysicalLink

case class ResourceConfig(
    operatorConfigs: Map[PhysicalOpIdentity, OperatorConfig],
    linkConfigs: Map[PhysicalLink, LinkConfig]
)
