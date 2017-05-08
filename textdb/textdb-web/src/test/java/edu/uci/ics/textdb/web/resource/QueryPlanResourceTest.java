package edu.uci.ics.textdb.web.resource;

import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.storage.RelationManager;
import edu.uci.ics.textdb.web.TextdbWebApplication;
import edu.uci.ics.textdb.web.TextdbWebConfiguration;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.glassfish.jersey.client.ClientProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by kishorenarendran on 11/3/16.
 */
public class QueryPlanResourceTest {
    @ClassRule
    public static final DropwizardAppRule<TextdbWebConfiguration> RULE =
            new DropwizardAppRule<>(TextdbWebApplication.class, ResourceHelpers.resourceFilePath("test-config.yml"));

    public static String queryPlanEndpoint = "http://localhost:%d/api/queryplan/execute";
    
    public static final String queryPlanRequestString = "{\n" +
            "    \"operators\": [{\n" +
            "        \"operator_id\": \"operator1\",\n" +
            "        \"operator_type\": \"DictionarySource\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"limit\": \"10\",\n" +
            "        \"offset\": \"100\",\n" +
            "        \"data_source\": \"query_plan_resource_test_table\",\n" +
            "        \"dictionary\": \"dict1\",\n" +
            "        \"matching_type\": \"PHRASE_INDEXBASED\"\n" +
            "    }, {\n" +
            "\n" +
            "        \"operator_id\": \"operator2\",\n" +
            "        \"operator_type\": \"TupleStreamSink\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"limit\": \"10\",\n" +
            "        \"offset\": \"100\"\n" +
            "    }],\n" +
            "    \"links\": [{\n" +
            "        \"from\": \"operator1\",\n" +
            "        \"to\": \"operator2\"    \n" +
            "    }]\n" +
            "}";

    public static final String faultyQueryPlanRequestString = "{\n" +
            "    \"operators\": [{\n" +
            "        \"operator_id\": \"operator1\",\n" +
            "        \"operator_type\": \"DictionaryMatcher\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"limit\": \"10\",\n" +
            "        \"offset\": \"100\",\n" +
            "        \"matching_type\": \"PHRASE_INDEXBASED\"\n" +
            "    }, {\n" +
            "\n" +
            "        \"operator_id\": \"operator2\",\n" +
            "        \"operator_type\": \"DictionaryMatcher\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"limit\": \"10\",\n" +
            "        \"offset\": \"100\",\n" +
            "        \"dictionary\": \"dict2\"\n" +
            "    }],\n" +
            "    \"links\": [{\n" +
            "        \"from\": \"operator1\",\n" +
            "        \"to\": \"operator2\"    \n" +
            "    }]\n" +
            "}";
    
    public static final String TEST_TABLE = "query_plan_resource_test_table";
    
    @BeforeClass
    public static void setUp() throws Exception {
        RelationManager.getRelationManager().createTable(TEST_TABLE, "../index/" + TEST_TABLE,
                new Schema(new Attribute("attributes", AttributeType.TEXT)), "standard");
    }
    
    @AfterClass
    public static void cleanUp() throws Exception {
        RelationManager.getRelationManager().deleteTable(TEST_TABLE);
    }

    /**
     * Tests the query plan execution endpoint.
     */
    @Test
    public void checkSampleEndpoint() {
        Client client = new JerseyClientBuilder(RULE.getEnvironment()).build("test client");
        client.property(ClientProperties.CONNECT_TIMEOUT, 5000);
        client.property(ClientProperties.READ_TIMEOUT,    5000);
        Response response = client.target(
                String.format(queryPlanEndpoint, RULE.getLocalPort()))
                .request()
                .post(Entity.entity(queryPlanRequestString, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(200);

        response = client.target(
                String.format(queryPlanEndpoint, RULE.getLocalPort()))
                .request()
                .post(Entity.entity(faultyQueryPlanRequestString, MediaType.APPLICATION_JSON));

        assertThat(response.getStatus()).isEqualTo(400);
    }
}
