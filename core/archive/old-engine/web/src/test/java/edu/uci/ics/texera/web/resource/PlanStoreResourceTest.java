package edu.uci.ics.texera.web.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.plangen.LogicalPlanTest;
import edu.uci.ics.texera.web.TexeraWebApplication;
import edu.uci.ics.texera.web.TexeraWebConfiguration;
import edu.uci.ics.texera.web.response.planstore.QueryPlanBean;
import edu.uci.ics.texera.web.response.planstore.QueryPlanListBean;
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
    public static final DropwizardAppRule<TexeraWebConfiguration> RULE =
            new DropwizardAppRule<>(TexeraWebApplication.class, ResourceHelpers.resourceFilePath("test-config.yml"));
    public static Client client;
    
    public static String planStoreEndpoint = "http://localhost:%d/api/planstore";
    
    
    public static String queryPlan1;
    public static String updatedQueryPlan1;
    public static String queryPlan2;
    
    static {
        try {
            queryPlan1 = new ObjectMapper().writeValueAsString(
                    new QueryPlanBean("plan1", "plan 1 description", LogicalPlanTest.getLogicalPlan1()));
            
            updatedQueryPlan1 = new ObjectMapper().writeValueAsString(
                    new QueryPlanBean("plan1", "updated plan 1 description", LogicalPlanTest.getLogicalPlan1()));
            
            queryPlan2 = new ObjectMapper().writeValueAsString(
                    new QueryPlanBean("plan2", "plan 2 description", LogicalPlanTest.getLogicalPlan2()));
        } catch (Exception e) {
            throw new TexeraException(e);
        }

    }

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
        QueryPlanListBean queryPlanResponse = mapper.readValue(response.readEntity(String.class),
                QueryPlanListBean.class);
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(queryPlanResponse.getQueryPlanList().size() == 2);
    }
}
