package edu.uci.ics.texera.web.resource

import edu.uci.ics.texera.workflow.common.metadata.{AllOperatorMetadata, OperatorMetadataGenerator}

import javax.annotation.security.RolesAllowed
import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, Produces}

@Path("/resources")
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Produces(Array(MediaType.APPLICATION_JSON))
class SystemMetadataResource {

  @GET
  @Path("/operator-metadata")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def getOperatorMetadata: AllOperatorMetadata = {
    OperatorMetadataGenerator.allOperatorMetadata
  }

}
