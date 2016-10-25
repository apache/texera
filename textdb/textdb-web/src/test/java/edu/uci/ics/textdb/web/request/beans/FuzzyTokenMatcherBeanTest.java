package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.web.request.beans.FileSinkBean;
import edu.uci.ics.textdb.web.request.beans.FuzzyTokenMatcherBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the FuzzyTokenMatcher operators' properties
 * Created by kishorenarendran on 10/20/16.
 */
public class FuzzyTokenMatcherBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final FuzzyTokenMatcherBean fuzzyTokenMatcherBean = new FuzzyTokenMatcherBean("operator1", "FuzzyTokenMatcher",
                "attributes", "10", "100", "query",  "0.8");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"FuzzyTokenMatcher\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"query\": \"query\",\n" +
                "    \"threshold_ratio\": \"0.8\"\n" +
                "}";
        FuzzyTokenMatcherBean deserializedObject = MAPPER.readValue(jsonString, FuzzyTokenMatcherBean.class);
        assertEquals(fuzzyTokenMatcherBean.equals(deserializedObject), true);
    }
}
