package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

trait ExecutionPlanGenerator {

  def generate() : (ExecutionPlan, PhysicalPlan)



}
