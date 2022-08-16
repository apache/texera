package edu.uci.ics.amber.engine.architecture.scheduling

import scala.collection.immutable.Set

case class SchedulingWork(regions: Set[PipelinedRegion], schedulingDurationInMs: Int = 0)
