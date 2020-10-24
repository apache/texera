package edu.uci.ics.texera.web.resource

import edu.uci.ics.texera.workflow.common.metadata.{AllOperatorMetadata, OperatorMetadataGenerator}
import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, Produces}

@Path("/resources")
@Produces(Array(MediaType.APPLICATION_JSON))
class SystemMetadataResource {

  @GET
  @Path("/operator-metadata") def getOperatorMetadata: AllOperatorMetadata = {
    OperatorMetadataGenerator.allOperatorMetadata
  }

}
