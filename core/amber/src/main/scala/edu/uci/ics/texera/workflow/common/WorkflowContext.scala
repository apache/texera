package edu.uci.ics.texera.workflow.common
import org.jooq.types.UInteger

class WorkflowContext {

  var jobId: String = _

  var userId: Option[UInteger] = None

  var wId: Int = _
}
