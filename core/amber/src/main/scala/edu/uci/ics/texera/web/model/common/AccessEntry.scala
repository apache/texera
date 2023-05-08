package edu.uci.ics.texera.web.model.common

import edu.uci.ics.texera.web.model.jooq.generated.enums.WorkflowUserAccessPrivilege

case class AccessEntry(userName: String, accessLevel: String) {}

case class AccessEntry2(email: String, name: String, privilege: WorkflowUserAccessPrivilege) {}
