package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.web.request.beans.RegexSourceBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the RegexSource operators' properties
 * Created by kishorenarendran on 10/20/16.
 */
public class RegexSourceBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final RegexSourceBean regexSourceBean = new RegexSourceBean("operator1", "RegexSource", "attributes",
                "10", "100", "regex", "datasource");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"RegexSource\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"regex\": \"regex\",\n" +
                "    \"data_source\": \"datasource\"\n" +
                "}";
        RegexSourceBean deserializedObject = MAPPER.readValue(jsonString, RegexSourceBean.class);
        assertEquals(regexSourceBean.equals(deserializedObject), true);
    }
}
