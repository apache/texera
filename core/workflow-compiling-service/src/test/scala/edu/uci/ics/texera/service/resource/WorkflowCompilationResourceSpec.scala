package edu.uci.ics.texera.service.resource

import io.dropwizard.testing.junit5.ResourceExtension
import jakarta.ws.rs.client.Entity
import jakarta.ws.rs.core.{MediaType, Response}
import org.assertj.core.api.Assertions.assertThat
import edu.uci.ics.amber.compiler.model.{LogicalLink, LogicalPlanPojo}
import edu.uci.ics.amber.operator.projection.{AttributeUnit, ProjectionOpDesc}
import edu.uci.ics.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.workflow.PortIdentity
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll
import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType}
import edu.uci.ics.amber.operator.filter.{
  ComparisonType,
  FilterOpDesc,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import edu.uci.ics.amber.operator.limit.LimitOpDesc

class WorkflowCompilationResourceSpec extends AnyFlatSpec with BeforeAndAfterAll {

  private val resources: ResourceExtension = ResourceExtension
    .builder()
    .addResource(new WorkflowCompilationResource())
    .setMapper(objectMapper)
    .build()
  override protected def beforeAll(): Unit = {
    resources.before()
  }

  override protected def afterAll(): Unit = {
    resources.after()
  }

  // utility function to create a csv scan op
  private def getCsvScanOpDesc(
      fileName: String,
      header: Boolean
  ): CSVScanSourceOpDesc = {
    val csvOp = new CSVScanSourceOpDesc()
    csvOp.fileName = Some(fileName)
    csvOp.customDelimiter = Some(",")
    csvOp.hasHeader = header
    csvOp
  }

  // utility function to create a projection op
  private def getProjectionOpDesc(
      attributeNames: List[String],
      isDrop: Boolean = false
  ): ProjectionOpDesc = {
    val projectionOpDesc = new ProjectionOpDesc()
    projectionOpDesc.attributes = attributeNames.map(name => new AttributeUnit(name, ""))
    projectionOpDesc.isDrop = isDrop
    projectionOpDesc
  }

  // utility function to create a limit op
  private def getLimitOpDesc(
      limit: Int
  ): LimitOpDesc = {
    val limitOpDesc = new LimitOpDesc
    limitOpDesc.limit = limit
    limitOpDesc
  }

  // utility function to create a filter op
  private def getFilterOpDesc(
      filterPredicates: java.util.List[FilterPredicate]
  ): FilterOpDesc = {
    val filterOpDesc = new SpecializedFilterOpDesc
    filterOpDesc.predicates = filterPredicates
    filterOpDesc
  }

  // utility function to transform a logical plan pojo to json that can be deserialized correctly by the compile endpoint
  private def transformLogicalPlanPojoToJsonString(logicalPlanPojo: LogicalPlanPojo): String = {
    val jsonNode = objectMapper.valueToTree[ObjectNode](logicalPlanPojo)

    // iterate over the "links" array and replace nested "id" fields
    val linksArray = jsonNode.withArray("links")
    linksArray.forEach { linkNode =>
      // replace "fromOpId" with its "id" field value
      val fromOpIdNode = linkNode.get("fromOpId")
      linkNode.asInstanceOf[ObjectNode].put("fromOpId", fromOpIdNode.get("id").asText())

      // replace "toOpId" with its "id" field value if it exists
      val toOpIdNode = linkNode.get("toOpId")
      linkNode.asInstanceOf[ObjectNode].put("toOpId", toOpIdNode.get("id").asText())
    }

    // convert the modified JSON node back to a string
    objectMapper.writeValueAsString(jsonNode)
  }

  // utility function for asserting the successful compilation
  private def assertSuccessfulCompilation(response: Response): WorkflowCompilationSuccess = {
    val responseBody = response.readEntity(classOf[String])
    val compilationResponse =
      objectMapper.readValue(responseBody, classOf[WorkflowCompilationResponse])

    assertThat(compilationResponse.asInstanceOf[WorkflowCompilationSuccess])
    compilationResponse.asInstanceOf[WorkflowCompilationSuccess]
  }

  it should "compile workflow successfully with multiple filter and limit operations" in {
    // construct the LogicalPlan: CSVScan --> Projection --> Limit --> Filter (TotalProfit > 10000) --> Filter (Region != "JPN") --> Limit
    val localCsvFilePath = "workflow-compiling-service/src/test/resources/country_sales_small.csv"
    val csvSourceOp = getCsvScanOpDesc(localCsvFilePath, header = true)
    val projectionOpDesc = getProjectionOpDesc(List("Region", "Total Profit"))
    val limitOpDesc1 = getLimitOpDesc(10)

    // Create the filter predicate for TotalProfit > 10000
    val filterPredicate1 = new FilterPredicate("Total Profit", ComparisonType.GREATER_THAN, "10000")
    val filterOpDesc1 = getFilterOpDesc(java.util.Arrays.asList(filterPredicate1))

    // Create the filter predicate for Region != "JPN"
    val filterPredicate2 = new FilterPredicate("Region", ComparisonType.NOT_EQUAL_TO, "JPN")
    val filterOpDesc2 = getFilterOpDesc(java.util.Arrays.asList(filterPredicate2))

    // Add a second limit operation
    val limitOpDesc2 = getLimitOpDesc(5)

    val logicalPlanPojo = LogicalPlanPojo(
      operators = List(
        csvSourceOp,
        projectionOpDesc,
        limitOpDesc1,
        filterOpDesc1,
        filterOpDesc2,
        limitOpDesc2
      ),
      links = List(
        LogicalLink(
          csvSourceOp.operatorIdentifier,
          PortIdentity(0),
          projectionOpDesc.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          projectionOpDesc.operatorIdentifier,
          PortIdentity(0),
          limitOpDesc1.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          limitOpDesc1.operatorIdentifier,
          PortIdentity(0),
          filterOpDesc1.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          filterOpDesc1.operatorIdentifier,
          PortIdentity(0),
          filterOpDesc2.operatorIdentifier,
          PortIdentity(0)
        ),
        LogicalLink(
          filterOpDesc2.operatorIdentifier,
          PortIdentity(0),
          limitOpDesc2.operatorIdentifier,
          PortIdentity(0)
        )
      ),
      opsToViewResult = List(),
      opsToReuseResult = List()
    )

    // transform the LogicalPlanPojo to a modified JSON string
    val modifiedLogicalPlanJsonString = transformLogicalPlanPojoToJsonString(logicalPlanPojo)

    // send the request to compile endpoint
    val response = resources
      .target("/compile")
      .request(MediaType.APPLICATION_JSON)
      .post(Entity.json(modifiedLogicalPlanJsonString))

    assertThat(response.getStatus).isEqualTo(200)

    // verify the schema is correctly propagated for the final limit operator
    val compilationResult = assertSuccessfulCompilation(response)
    val finalLimitInputSchema =
      compilationResult.operatorInputSchemas.get(limitOpDesc2.operatorIdentifier.id)
    assert(
      finalLimitInputSchema.get.head.get.equals(
        List(
          new Attribute("Region", AttributeType.STRING),
          new Attribute("Total Profit", AttributeType.DOUBLE)
        )
      )
    )
  }
}
