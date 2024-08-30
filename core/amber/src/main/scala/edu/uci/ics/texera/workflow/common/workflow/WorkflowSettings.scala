package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.common.AmberConfig

object WorkflowSettings {
  def apply(batchSize: Int = AmberConfig.defaultBatchSize): WorkflowSettings = new WorkflowSettings(batchSize)
}

class WorkflowSettings(var batchSize: Int = AmberConfig.defaultBatchSize)