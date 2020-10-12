package web.resource

import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, Produces}
import texera.common.metadata.{AllOperatorMetadata, OperatorMetadataGenerator}

@Path("/resources")
@Produces(Array(MediaType.APPLICATION_JSON))
class SystemMetadataResource {

  @GET
  @Path("/operator-metadata") def getOperatorMetadata: AllOperatorMetadata = {
    OperatorMetadataGenerator.allOperatorMetadata
  }

}
