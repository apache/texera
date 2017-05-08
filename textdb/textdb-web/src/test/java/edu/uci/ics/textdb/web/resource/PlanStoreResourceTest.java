package edu.uci.ics.textdb.web.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.web.TextdbWebApplication;
import edu.uci.ics.textdb.web.TextdbWebConfiguration;
import edu.uci.ics.textdb.web.response.QueryPlanResponse;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.glassfish.jersey.client.ClientProperties;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Kishore Narendran
 * Created on 3/6/17.
 */
public class PlanStoreResourceTest {
    @ClassRule
    public static final DropwizardAppRule<TextdbWebConfiguration> RULE =
            new DropwizardAppRule<>(TextdbWebApplication.class, ResourceHelpers.resourceFilePath("test-config.yml"));
    public static Client client;
    
    public static String planStoreEndpoint = "http://localhost:%d/api/planstore";

    public static final String queryPlan1 = "  {\n" +
            "  \"name\": \"plan1\",\n" +
            "  \"description\": \"basic dictionary source plan\",\n" +
            "  \"query_plan\": {\n" +
            "    \"operators\": [\n" +
            "      {\n" +
            "        \"operator_type\": \"DictionarySource\",\n" +
            "        \"operator_id\": \"operator1\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"limit\": \"10\",\n" +
            "        \"offset\": \"100\",\n" +
            "        \"dictionary\": \"dict1\",\n" +
            "        \"matching_type\": \"PHRASE_INDEXBASED\",\n" +
            "        \"data_source\": \"query_plan_resource_test_table\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"operator_type\": \"TupleStreamSink\",\n" +
            "        \"operator_id\": \"operator2\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"limit\": \"10\",\n" +
            "        \"offset\": \"100\"\n" +
            "      }\n" +
            "    ],\n" +
            "    \"links\": [\n" +
            "      {\n" +
            "        \"from\": \"operator1\",\n" +
            "        \"to\": \"operator2\"\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}";

    public static final String updatedQueryPlan1 = "  {\n" +
            "  \"name\": \"plan1\",\n" +
            "  \"description\": \"basic plan\",\n" +
            "  \"query_plan\": {\n" +
            "    \"operators\": [\n" +
            "      {\n" +
            "        \"operator_type\": \"DictionarySource\",\n" +
            "        \"operator_id\": \"operator1\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"dictionary\": \"changed_dict\",\n" +
            "        \"matching_type\": \"PHRASE_INDEXBASED\",\n" +
            "        \"data_source\": \"query_plan_resource_test_table\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"operator_type\": \"TupleStreamSink\",\n" +
            "        \"operator_id\": \"operator2\",\n" +
            "        \"attributes\": \"attributes\"\n" +
            "      }\n" +
            "    ],\n" +
            "    \"links\": [\n" +
            "      {\n" +
            "        \"from\": \"operator1\",\n" +
            "        \"to\": \"operator2\"\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}";

    public static final String queryPlan2 = "{\n" +
            "      \"name\": \"plan2\",\n" +
            "      \"description\": \"basic dictionary source plan\",\n" +
            "      \"query_plan\": {\n" +
            "        \"operators\": [\n" +
            "          {\n" +
            "            \"operator_type\": \"DictionarySource\",\n" +
            "            \"operator_id\": \"operator1\",\n" +
            "            \"attributes\": \"attributes\",\n" +
            "            \"limit\": \"10\",\n" +
            "            \"offset\": \"100\",\n" +
            "            \"dictionary\": \"dict1\",\n" +
            "            \"matching_type\": \"PHRASE_INDEXBASED\",\n" +
            "            \"data_source\": \"query_plan_resource_test_table\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"operator_type\": \"TupleStreamSink\",\n" +
            "            \"operator_id\": \"operator2\",\n" +
            "            \"attributes\": \"attributes\",\n" +
            "            \"limit\": \"10\",\n" +
            "            \"offset\": \"100\"\n" +
            "          }\n" +
            "        ],\n" +
            "        \"links\": [\n" +
            "          {\n" +
            "            \"from\": \"operator1\",\n" +
            "            \"to\": \"operator2\"\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    }";

    public static boolean checkJsonEquivalence(String json1, String json2) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode tree1 = mapper.readTree(json1);
        JsonNode tree2 = mapper.readTree(json2);
        return tree1.equals(tree2);
    }

    @BeforeClass
    public static void setUpClient() {
        client = new JerseyClientBuilder(RULE.getEnvironment()).build("test client");
        client.property(ClientProperties.CONNECT_TIMEOUT, 5000);
        client.property(ClientProperties.READ_TIMEOUT,    5000);
    }

    @Before
    public void deleteQueryPlans() {
        client.target(
                String.format(planStoreEndpoint + "/plan1", RULE.getLocalPort()))
                .request()
                .delete();
        client.target(
                String.format(planStoreEndpoint + "/plan2", RULE.getLocalPort()))
                .request()
                .delete();
    }

    @Test
    public void checkAddOnePlan() throws IOException{
        Response response = client.target(
                String.format(planStoreEndpoint, RULE.getLocalPort()))
                .request()
                .post(Entity.entity(queryPlan1, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(200);

        response = client.target(
                String.format(planStoreEndpoint + "/plan1", RULE.getLocalPort()))
                .request()
                .get();
        String returnedJson = response.readEntity(String.class);
        assertThat(checkJsonEquivalence(queryPlan1, returnedJson));
        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    public void checkUpdatePlan() throws IOException {

        Response response = client.target(
                String.format(planStoreEndpoint, RULE.getLocalPort()))
                .request()
                .post(Entity.entity(queryPlan1, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(200);

        response = client.target(
                String.format(planStoreEndpoint + "/plan1", RULE.getLocalPort()))
                .request()
                .put(Entity.entity(updatedQueryPlan1, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(200);

        response = client.target(
                String.format(planStoreEndpoint + "/plan1", RULE.getLocalPort()))
                .request()
                .get();
        String returnedJson = response.readEntity(String.class);
        assertThat(checkJsonEquivalence(updatedQueryPlan1, returnedJson));
        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    public void checkDeletePlan() {
        Response response = client.target(
                String.format(planStoreEndpoint, RULE.getLocalPort()))
                .request()
                .post(Entity.entity(queryPlan2, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(200);

        response = client.target(
                String.format(planStoreEndpoint + "/plan2", RULE.getLocalPort()))
                .request()
                .delete();

        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    public void checkAddMultiplePlans() throws IOException{
        Response response = client.target(
                String.format(planStoreEndpoint, RULE.getLocalPort()))
                .request()
                .post(Entity.entity(queryPlan2, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(200);

        response = client.target(
                String.format(planStoreEndpoint, RULE.getLocalPort()))
                .request()
                .post(Entity.entity(queryPlan1, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(200);

        response = client.target(
                String.format(planStoreEndpoint, RULE.getLocalPort()))
                .request()
                .get();

        ObjectMapper mapper = new ObjectMapper();
        QueryPlanResponse queryPlanResponse = mapper.readValue(response.readEntity(String.class),
                QueryPlanResponse.class);
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(queryPlanResponse.getQueryPlans().size() == 2);
    }
}
