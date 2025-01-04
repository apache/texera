package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.core.workflow.PhysicalLink

case class ResourceConfig(
    operatorConfigs: Map[PhysicalOpIdentity, OperatorConfig],
    linkConfigs: Map[PhysicalLink, LinkConfig]
)
