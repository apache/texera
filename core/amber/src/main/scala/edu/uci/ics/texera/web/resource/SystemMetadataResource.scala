package edu.uci.ics.texera.web.resource

import edu.uci.ics.texera.workflow.common.metadata.{AllOperatorMetadata, OperatorMetadataGenerator}
import javax.annotation.security.RolesAllowed
import javax.ws.rs.{GET, Path}
class SystemMetadataResource {

  @GET
  @Path("/resources/operator-metadata")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def getOperatorMetadata: AllOperatorMetadata = {
    OperatorMetadataGenerator.allOperatorMetadata
  }
}
