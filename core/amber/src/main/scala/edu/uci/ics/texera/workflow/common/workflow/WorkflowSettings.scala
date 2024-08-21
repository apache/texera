package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.texera.workflow.common.workflow.WorkflowSettings.DEFAULT_BATCH_SIZE

object WorkflowSettings {
  val DEFAULT_BATCH_SIZE = 400
  def apply(batchSize: Int = DEFAULT_BATCH_SIZE): WorkflowSettings = new WorkflowSettings(batchSize)
}
class WorkflowSettings(var batchSize: Int = DEFAULT_BATCH_SIZE)
