package edu.uci.ics.texera.service.resource

import io.dropwizard.testing.junit5.ResourceExtension
import jakarta.ws.rs.client.Entity
import jakarta.ws.rs.core.MediaType
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.Mockito.mock
import org.assertj.core.api.Assertions.assertThat
import edu.uci.ics.amber.compiler.model.LogicalPlanPojo
import edu.uci.ics.amber.compiler.WorkflowCompiler
import edu.uci.ics.amber.operator.source.scan.csv.CSVScanSourceOpDesc

class WorkflowCompilationResourceTest {

  // Mock the WorkflowCompiler dependency
  private val mockCompiler = mock(classOf[WorkflowCompiler])

  // Initialize ResourceExtension with the resource to be tested
  private val resources: ResourceExtension = ResourceExtension
    .builder()
    .addResource(new WorkflowCompilationResource())
    .build()

  @BeforeEach
  def setup(): Unit = {
    resources.before()
  }

  @AfterEach
  def tearDown(): Unit = {
    resources.after()
  }

  @Test
  def testCompileWorkflowSuccess(): Unit = {
    val localCsvFilePath = "workflow-core/src/test/resources/country_sales_small.csv"
    val csvSourceOp = new CSVScanSourceOpDesc
    csvSourceOp.fileName = Some(localCsvFilePath)
    csvSourceOp.operatorIdentifier
    // Create a mock LogicalPlanPojo instance
    val logicalPlanPojo = LogicalPlanPojo(
      List(
        csvSourceOp
      ),
      List(),
      List(),
      List()
    )

    // Mock the compilation result
    val mockResponse = WorkflowCompilationSuccess(
      physicalPlan = null, // Use a proper mock or test instance
      operatorInputSchemas = Map.empty
    )

    // Make a POST request to the endpoint
    val response = resources
      .target("/compile")
      .request(MediaType.APPLICATION_JSON)
      .post(Entity.json(logicalPlanPojo))

    // Assert the status code
    assertThat(response.getStatus).isEqualTo(200)

    // Assert the response body
    val responseBody = response.readEntity(classOf[WorkflowCompilationResponse])
    assertThat(responseBody).isInstanceOf(classOf[WorkflowCompilationSuccess])
  }
}
