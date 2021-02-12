package edu.uci.ics.texera.workflow.common
import org.jooq.types.UInteger

class WorkflowContext {

  var jobID: String = _

  var userID: Option[UInteger] = None

}
