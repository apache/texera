package edu.uci.ics.textdb.web.request.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.web.request.beans.JoinBean;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the deserialization of the JoinBean operators' properties
 * Created by kishorenarendran on 10/20/16.
 */
public class JoinBeanTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void testDeserialization() throws IOException {
        final JoinBean joinBean = new JoinBean("operator1", "Join", "attributes", "10", "100", "attribute", "10");
        String jsonString = "{\n" +
                "    \"operator_id\": \"operator1\",\n" +
                "    \"operator_type\": \"Join\",\n" +
                "    \"attributes\":  \"attributes\",\n" +
                "    \"limit\": \"10\",\n" +
                "    \"offset\": \"100\",\n" +
                "    \"id_attribute\": \"attribute\",\n" +
                "    \"distance\": \"10\"\n" +
                "}";
        JoinBean deserializedObject = MAPPER.readValue(jsonString, JoinBean.class);
        assertEquals(joinBean.equals(deserializedObject), true);
    }
}
