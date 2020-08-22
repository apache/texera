package web.resource

import java.nio.file.Files

import com.fasterxml.jackson.databind.JsonNode
import javax.ws.rs.{GET, Path, Produces}
import javax.ws.rs.core.MediaType
import texera.common.TexeraUtils
import texera.common.schema.{GroupOrder, OperatorGroupConstants, OperatorSchemaGenerator}

import scala.beans.BeanProperty
import scala.collection.JavaConverters

@Path("/resources")
@Produces(Array(MediaType.APPLICATION_JSON))
class SystemMetadataResource {

  case class OperatorMetadata(
      @BeanProperty operators: List[JsonNode],
      @BeanProperty groups: List[GroupOrder]
  )

  @GET
  @Path("/operator-metadata") def getOperatorMetadata: OperatorMetadata = {
    val objectMapper = TexeraUtils.objectMapper;
    val operators = JavaConverters
      .asScalaSet(OperatorSchemaGenerator.operatorTypeMap.keySet())
      .map(operatorClass =>
        objectMapper.readTree(
          Files.readAllBytes(OperatorSchemaGenerator.getJsonSchemaPath(operatorClass))
        )
      )
      .toList
    val groups = JavaConverters.asScalaBuffer(OperatorGroupConstants.OperatorGroupOrderList).toList
    OperatorMetadata(operators, groups)
  }

}
