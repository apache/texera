package edu.uci.ics.textdb.web.request;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Checks the deserialization of a query plan execution request
 * Created by kishorenarendran on 10/20/16.
 */
public class QueryPlanRequestTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        String jsonString = "{\n" +
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
        QueryPlanRequest queryPlanRequest = MAPPER.readValue(jsonString, QueryPlanRequest.class);
        assertEquals(queryPlanRequest.getOperators().size(), 2);
        assertEquals(queryPlanRequest.getLinks().size(), 1);
    }
}
