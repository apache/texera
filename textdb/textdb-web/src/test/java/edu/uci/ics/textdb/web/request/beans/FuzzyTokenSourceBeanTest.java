package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.web.request.beans.FuzzyTokenSourceBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the FuzzyTokenSource operators' properties
 * Created by kishorenarendran on 10/20/16.
 */
public class FuzzyTokenSourceBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final FuzzyTokenSourceBean fuzzyTokenSourceBean = new FuzzyTokenSourceBean("operator1", "FuzzyTokenSource",
                "attributes", "10", "100", "query", "0.8", "datasource");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"FuzzyTokenSource\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"query\": \"query\",\n" +
                "    \"threshold_ratio\": \"0.8\",\n" +
                "    \"data_source\": \"datasource\"\n" +
                "}";
        FuzzyTokenSourceBean deserializedObject = MAPPER.readValue(jsonString, FuzzyTokenSourceBean.class);
        assertEquals(fuzzyTokenSourceBean.equals(deserializedObject), true);
    }
}
