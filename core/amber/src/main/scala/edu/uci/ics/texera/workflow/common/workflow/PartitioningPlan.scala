package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity

class PartitioningPlan(val strategies: Map[LinkIdentity, LinkStrategy]) {}
