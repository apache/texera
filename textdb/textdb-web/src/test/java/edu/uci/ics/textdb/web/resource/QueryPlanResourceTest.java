package edu.uci.ics.textdb.web.resource;

import edu.uci.ics.textdb.web.TextdbWebApplication;
import edu.uci.ics.textdb.web.TextdbWebConfiguration;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.glassfish.jersey.client.ClientProperties;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the execution of a sample query plan
 * Created by kishorenarendran on 10/21/16.
 */
public class QueryPlanResourceTest {

    private static final String QUERY_PLAN_JSON_STRING = "{\n" +
            "    \"operators\": [{\n" +
            "        \"operator_id\": \"operator1\",\n" +
            "        \"operator_type\": \"RegexSource\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"limit\": \"10\",\n" +
            "        \"offset\": \"100\",\n" +
            "        \"regex\": \"regex\",\n" +
            "        \"data_source\": \"datasource\"\n" +
            "    }, {\n" +
            "\n" +
            "        \"operator_id\": \"operator2\",\n" +
            "        \"operator_type\": \"KeywordMatcher\",\n" +
            "        \"attributes\": \"attributes\",\n" +
            "        \"limit\": \"10\",\n" +
            "        \"offset\": \"100\",\n" +
            "        \"keyword\": \"keyword\",\n" +
            "        \"matching_type\": \"PHRASE_INDEXBASED\"\n" +
            "    }],\n" +
            "    \"links\": [{\n" +
            "        \"from\": \"operator1\",\n" +
            "        \"to\": \"operator2\"    \n" +
            "    }]\n" +
            "}";

    @ClassRule
    public static final DropwizardAppRule<TextdbWebConfiguration> RULE =
            new DropwizardAppRule<>(TextdbWebApplication.class, ResourceHelpers.resourceFilePath("test-config.yml"));

    @Test
    public void checkQueryPlanEndpoint() {
        Client client = new JerseyClientBuilder(RULE.getEnvironment()).build("test client");
        client.property(ClientProperties.CONNECT_TIMEOUT, 5000);
        client.property(ClientProperties.READ_TIMEOUT,    5000);

        Response response = client.target(
                String.format("http://localhost:%d/queryplan/execute", RULE.getLocalPort()))
                .request()
                .post(Entity.json(QUERY_PLAN_JSON_STRING));

        assertThat(response.getStatus()).isEqualTo(200);
    }
}
