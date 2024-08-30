package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.common.AmberConfig

object WorkflowSettings {
  def apply(dataTransferBatchSize: Int = AmberConfig.defaultBatchSize): WorkflowSettings = new WorkflowSettings(dataTransferBatchSize)
}

class WorkflowSettings(var dataTransferBatchSize: Int = AmberConfig.defaultBatchSize)